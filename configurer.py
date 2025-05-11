import json
import datetime
import os
import re
import shutil
from minio import Minio
from confluent_kafka import Consumer, KafkaException
from confluent_kafka import Producer
import pandas as pd
import lightgbm as lgb
from sklearn.feature_selection import mutual_info_regression
import random
import sys

kafka_config = {
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'task-to-collect-result',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(kafka_config)
consumer.subscribe(['task-to-configure-queue-topic'])
producer = Producer(kafka_config)

minio_client = Minio(
    "localhost:9000",
    access_key="JH71e6ukpwLjJycLuwvj",
    secret_key="KfGI4sc3qAIHNddapLVvBCk8QXO09tKTvIDPwthl", secure=False
)

def process_config_change_log_data(config, init_timestamp, data):
    current_config = []
    for i, x in enumerate(config["params"]):
        tmp = [x["name"], x["value"]]
        if (i == 0):
            tmp.append(datetime.datetime.fromtimestamp(init_timestamp / 1e3).strftime("%Y-%m-%dT%H:%M:%SZ"))
        current_config.append(tmp)
    configs.append([x[:] for x in current_config])
    decorated = [(x[2], i, x) for i, x in enumerate(data)]
    decorated.sort()
    data = [x for timestamp, i, x in decorated]
    for x in data:
        current_config = list(current_config[:])
        for y in current_config:
            if y[0] == x[0]:
                y[1] = x[1]
                current_config[0][2] = datetime.datetime.fromtimestamp(x[2] / 1e3).strftime("%Y-%m-%dT%H:%M:%SZ")
                configs.append([j[:] for j in current_config])
                break
    for x in configs:
        for y in x:
            if len(y) > 2:
                cycle_start_timestamps.append(y[2])

def process_workload_data(data):
    for x in data:
        throughput_ranges.append((x["leftBoundary"], x["rightBoundary"]))
    for x in throughput_ranges:
        metric_data_grouped_by_through_ranges[x] = []
        deltas[x] = [0]
        avgs[x] = [0]

def grouping_metric_data_by_throughput_ranges(data):
    for x in content:
        for y in throughput_ranges:
            if y[0] <= float(x.split(';')[-1][:-1].replace(',', '.')) < y[1]:
                metric_data_grouped_by_through_ranges[y].append(x)
                break

def evaluate_deltas():
    for x in throughput_ranges:
        print(f'range: {x}')
        for y in range(len(cycle_start_timestamps)):
            if y % 50 == 0:
                print(f'cycle: {y}')
            sum = 0
            count = 0
            range_timestamp_start = datetime.datetime.strptime(cycle_start_timestamps[y].split('.')[0], '%Y-%m-%dT%H:%M:%SZ')
            if y == len(cycle_start_timestamps) - 1:
                range_timestamp_end = datetime.datetime.max
            else:
                range_timestamp_end = datetime.datetime.strptime(cycle_start_timestamps[y + 1].split('.')[0],
                                                                 '%Y-%m-%dT%H:%M:%SZ')
            for j in range(len(metric_data_grouped_by_through_ranges[x])):
                cur_timestamp = datetime.datetime.strptime(
                    metric_data_grouped_by_through_ranges[x][j].split(';')[0].split('.')[0], '%Y-%m-%dT%H:%M:%SZ')
                if range_timestamp_start <= cur_timestamp < range_timestamp_end:
                    sum += float(metric_data_grouped_by_through_ranges[x][j].split(';')[1].replace(',', '.'))
                    count += 1
                elif cur_timestamp >= range_timestamp_end:
                    break
            if y != 0:
                if (count != 0):
                    deltas[x].append((sum / count) - avgs[x][y - 1])
                    avgs[x].append((sum / count))
                else:
                    deltas[x].append(deltas[x][-1])
                    avgs[x].append(avgs[x][-1])
            else:
                if (count != 0):
                    avgs[x][0] = sum / count

def prepare_fitting_file(r):
    if len(param_names) == 0:
        largest_config = []
        for x in configs:
            if len(x) > len(largest_config):
                largest_config = x
        for x in largest_config:
            param_names.append(x[0])
    header = ';'.join(param_names) + f';delta\n'
    with open("tmp/fitting_data.csv", "w") as file:
        file.write(header)
        for x in range(len(configs)):
            param_values = []
            for y in configs[x]:
                param_values.append(str(y[1]))
            row = ';'.join(param_values) + f';{str(deltas[r][x])}\n'
            file.write(row)

def fit_model():
    data = pd.read_csv("tmp/fitting_data.csv", sep=';')
    data_train = data[:len(data * 0.8)].astype(float)
    data_test = data[len(data * 0.8):].astype(float)
    print(data.columns.tolist())
    X_train = data_train.drop(columns=["delta"])
    y_train = data_train["delta"]
    X_test = data_train.drop(columns=["delta"])
    y_test = data_train["delta"]
    mi = mutual_info_regression(X_train, y_train)
    print(mi)
    well_correlated_params = {}
    correalation = data.corr()["delta"].sort_index()

    for x in range(len(param_names)):
        if abs(correalation[param_names[x]]) >= 0.25:
            well_correlated_params[param_names[x]] = correalation[param_names[x]]
    print(well_correlated_params)
    # Обучение модели
    model = lgb.LGBMRegressor(
        boosting_type='gbdt',
        num_leaves=255,  # Увеличить для больших данных
        max_depth=-1,  # Автоматическое определение
        learning_rate=0.05,  # Уменьшенный learning rate
        n_estimators=2000,  # Больше деревьев
        min_child_samples=50,  # Увеличить для борьбы с шумом
        subsample=0.8,  # Случайное подвыборка строк
        colsample_bytree=0.8,  # Случайный выбор признаков
        reg_alpha=0.1,  # L1 регуляризация
        reg_lambda=0.1,  # L2 регуляризация
        random_state=42,
        n_jobs=-1  # Использовать все ядра
    )  # Увеличьте для сложных данных)
    model.fit(X_train, y_train)
    return [X_train, well_correlated_params, model]

def get_optimal_config(X_train, well_correlated_params, model):
    def gen_configs(param_index, existing_rows, params_to_generate):
        min_value = param_names_and_metadata[params_to_generate[param_index]][0]
        max_value = param_names_and_metadata[params_to_generate[param_index]][1]
        step = param_names_and_metadata[params_to_generate[param_index]][2]
        i = min_value + step
        generated_rows = []
        while i <= max_value:
            for j in existing_rows:
                if isinstance(param_names_and_positions[params_to_generate[param_index]], list):
                    l = param_names_and_positions[params_to_generate[param_index]][0]
                else:
                    l = param_names_and_positions[params_to_generate[param_index]]
                tmp = j.copy()
                tmp[l] = i
                generated_rows.append(tmp)
            i += step
        existing_rows += generated_rows
        if param_index == len(params_to_generate) - 1:
            return existing_rows
        return gen_configs(param_index + 1, existing_rows, params_to_generate)

    existing_rows = [X_train.__array__()[-1].copy()]
    for x in list(well_correlated_params.keys()):
        if isinstance(param_names_and_positions[x], list):
            j = param_names_and_positions[x][0]
        else:
            j = param_names_and_positions[x]
        existing_rows[0][j] = float(0)
    # print(gen_configs(0, existing_rows, list(well_correlated_params.keys())))
    h = None
    if len(well_correlated_params) > 0:
        h = gen_configs(0, existing_rows, list(well_correlated_params.keys()))
    else:
        h = existing_rows
    print(len(h))
    print(h[0])
    max_predicted = -sys.float_info.max
    max_predicted_X = []
    configs_to_try_count = 1000
    for x in range(configs_to_try_count):
        if x % 100 == 0:
            print(x)
        cfg = random.choice(h)
        tmp = model.predict(cfg.reshape(1, -1))[0]
        if tmp > max_predicted:
            max_predicted = tmp
            max_predicted_X = cfg
    print(max_predicted_X)
    print(max_predicted)
    return [max_predicted_X, max_predicted]

def process_result(param_values, predicted_delta, r):
    workload_type_name = ""
    for x in parsed_message["workloadProfile"]["workloadTypes"]:
        if x["leftBoundary"] == r[0] and x["rightBoundary"] == r[1]:
            workload_type_name = x["name"]
    config = dict()
    param_names = list(param_names_and_positions.keys())
    for i, x in enumerate(param_values):
        if isinstance(param_names_and_positions[param_names[i]], list):
            param_names_and_positions[param_names[i]] = param_names_and_positions[param_names[i]][0]
        config[param_names[i]] = param_values[param_names_and_positions[param_names[i]]]
    config["predicted_delta"] = predicted_delta
    if not minio_client.bucket_exists("configuration"):
        minio_client.make_bucket("configuration")
    filename = f"tmp/request_to_configure_id_{parsed_message["requestToConfigureId"]}_{workload_type_name}.json"
    with open(filename, 'w') as file:
        file.write(json.dumps(config))
    minio_client.fput_object("configuration", file_path=filename, object_name=filename.split('/')[1])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            pass
        else:
            print(f"Received message: {msg.value().decode('utf-8')}")
            parsed_message = json.loads(msg.value().decode('utf-8'))
            filenames = parsed_message['filenames']
            content = []
            configs = []
            cycle_start_timestamps = []
            throughput_ranges = []
            metric_data_grouped_by_through_ranges = {}
            deltas = {}
            avgs = {}
            model = None
            param_names = []
            param_names_and_metadata = dict()
            for filename in filenames:
                try:
                    minio_client.fget_object("collector", filename, "tmp/" + filename)
                    with open("tmp/" + filename, "r") as file:
                        content += file.readlines()
                        #data[';'.join(content[0].replace('\n', '').split(';')[2:])] = content
                except:
                    print("Failed to download a file: " + filename)
            # process data here...
            decorated = [(datetime.datetime.strptime(re.sub(r'\++', 'Z', x.split(";")[0]), '%Y-%m-%dT%H:%M:%SZ'), i, x) for i, x in enumerate(content)]
            decorated.sort()
            content = [x for timestamp, i, x in decorated]
            param_names_and_positions = dict()
            param_values_change_log = []
            first_param_name = ""
            for i, x in enumerate(parsed_message["config"]["params"]):
                param_names_and_positions[x["name"]] = i
                param_names_and_metadata[x["name"]] = [x["minValue"], x["maxValue"], x["step"]]
                if i == 0:
                    param_names_and_positions[x["name"]] = [param_names_and_positions[x["name"]], min(y["creationTimestamp"] for y in parsed_message["config"]["params"])]
                    first_param_name = x["name"]
                for y in x["configChangeLogs"]:
                    param_values_change_log.append((x["name"], y["newValue"], y["updateTimestamp"]))
            print(content)
            process_config_change_log_data(parsed_message["config"], param_names_and_positions[first_param_name][1], param_values_change_log)
            process_workload_data(parsed_message["workloadProfile"]["workloadTypes"])
            grouping_metric_data_by_throughput_ranges(content)
            evaluate_deltas()
            for r in throughput_ranges:
                prepare_fitting_file(r)
                tmp = fit_model()
                tmp = get_optimal_config(tmp[0], tmp[1], tmp[2])
                process_result(tmp[0], tmp[1], r)
            shutil.rmtree("./tmp", ignore_errors=False)
            producer.produce("task-to-configure-result-topic",
                             headers=[("__TypeId__", "org.kubsu.tuning.domain.dto.TaskToConfigureResultDto")],
                             value=json.dumps({"taskId": parsed_message["requestToConfigureId"], "taskResult": "OK"}))

except KeyboardInterrupt:
    producer.produce("task-to-configure-result-topic",
                     headers=[("__TypeId__", "org.kubsu.tuning.domain.dto.TaskToConfigureResultDto")],
                     value=json.dumps({"taskId": parsed_message["requestToConfigureId"], "taskResult": "ERROR"}))
    pass
finally:
    consumer.close()
