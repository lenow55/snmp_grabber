from argparse import ArgumentParser, ArgumentTypeError
# from ahocorasic_parser import SNMP_Parser_AHOCK
from datetime import datetime
from io import BytesIO
import os
import pyarrow
from pyarrow import csv
from pyarrow import parquet as pa_pq
from pathlib import Path

from snmp_daemon import snmpDaemon

# parser: ArgumentParser = ArgumentParser(prog='gamma_n.py')
# parser.add_argument("mode",
#                    type=str,
#                    choices=["encrypt","decrypt"],
#                    help="Указывает тип выполняемой операции")
def check_range_timedelta(value):
    ivalue = int(value)
    if ivalue < 60 or ivalue > 3600:
        raise ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue

def validate_ip_address(address: str) -> str:
    parts = address.split(".")

    if len(parts) != 4:
        raise ArgumentTypeError("IP address {} is not valid".format(address))

    for part in parts:
        if not isinstance(int(part), int):
            raise ArgumentTypeError("IP address {} is not valid".format(address))

        if int(part) < 0 or int(part) > 255:
            raise ArgumentTypeError("IP address {} is not valid".format(address))

    print("IP address {} is valid".format(address))
    return address

parser: ArgumentParser = ArgumentParser(
    prog='batch_write_utils.py')
parser.add_argument("-p",
                    "--path",
                    type=str,
                    required=True,
                    default="",
                    help="путь до csv файлика")
parser.add_argument(
    "-o",
    "--output",
    type=str, required=True,
    default="./output",
    help="путь до папки вывода файлов интерфейсов")
parser.add_argument(
    "-t",
    "--timedelta",
    type=check_range_timedelta, required=False,
    default=300,
    help="Время в секундах между повторными запросами от 60 до 1h")
parser.add_argument(
    "agent_ip",
    type=validate_ip_address,
    required=True,
    help="ip узла snmp")
# args = parser.parse_args()
#
#
# with open(args.path, 'r') as file:
#    lines = file.readlines()
#
# result = extract_values_from_log(lines)
# result.to_csv(args.output, index=False)  # Преобразование DataFrame в файл CSV

if __name__ == '__main__':
    args = parser.parse_args()
    snmp_daemon = snmpDaemon(args.agent_ip, args.timedelta)
    snmp_daemon.initialize_first_request()
#    source = BytesIO()
#    print(args.path)
#    table: pyarrow.Table = csv.read_csv(args.path)
#    batches = table.to_batches(1)
#    Path(args.output).mkdir(parents=True, exist_ok=True)
#    for index, batch in enumerate(batches, 1):
#        writer = pa_pq.ParquetWriter(os.path.join(
#            args.output, f"interface_{index}.parquet"),
#            table.schema)
#        writer.write_batch(batch)
#        writer.close()
