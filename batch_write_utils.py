from argparse import ArgumentParser, ArgumentTypeError
import pyarrow
from time import sleep

import sys
import os

def check_range_timedelta(value):
    ivalue = int(value)
    if ivalue < 5 or ivalue > 3600:
        raise ArgumentTypeError(
            "%s is an invalid positive int value" % value)
    return ivalue


def validate_ip_address(address: str) -> str:
    parts = address.split(".")

    if len(parts) != 4:
        raise ArgumentTypeError(
            "IP address {} is not valid".format(address))

    for part in parts:
        if not isinstance(int(part), int):
            raise ArgumentTypeError(
                "IP address {} is not valid".format(address))

        if int(part) < 0 or int(part) > 255:
            raise ArgumentTypeError(
                "IP address {} is not valid".format(address))

    return address


parser: ArgumentParser = ArgumentParser(
    prog='batch_write_utils.py')
parser.add_argument(
    "-o",
    "--output",
    type=str, required=True,
    default="./output",
    help="путь до папки вывода файлов интерфейсов")
parser.add_argument(
    "-t", "--timedelta", type=check_range_timedelta,
    required=False, default=300,
    help="Время в секундах между повторными запросами от 5sec до 1h")
parser.add_argument(
    "-d",
    "--debug",
    type=str,
    choices=["debug", "info"],
    default="debug",
    help="уровень отладочной информации"
)
parser.add_argument(
    "-c", "--check_only", action='store_true',
    help="Просмотреть схему и выйти")
parser.add_argument(
    "agent_ip",
    type=validate_ip_address,
    help="ip узла snmp")

def main(argv):
    args = parser.parse_args()
    timeout_exit_process: int = 20
    if args.timedelta < 21:
        timeout_exit_process = 4
    from logger import conf_logger
    conf_logger(args.debug)

    import logging
    logger = logging.getLogger(__name__)

    from snmp_daemon import snmpDaemon
    snmp_daemon = snmpDaemon(args.agent_ip, args.timedelta, timeout_exit_process)
    try:
        check_schema: pyarrow.Schema = snmp_daemon.initialize_first_request()
    except Exception as e:
        logger.fatal(f"Can't init daemon with {e}")
        return 1
    logger.debug("This schema will be used")
    logger.debug(check_schema)
    if args.check_only:
        return 0

    check_count_interfaces: int = snmp_daemon.initialize_writers(
        args.output)
    logger.info(f"Будет получена информация\
            о {check_count_interfaces} интерфейсах")

    snmp_daemon.start()
    try:
        while snmp_daemon.is_alive():
            sleep(600)
    except KeyboardInterrupt:
        snmp_daemon.shutdown()
        snmp_daemon.join(timeout_exit_process)
        if snmp_daemon.is_alive():
            snmp_daemon.close_writers()
            try:
                logger.info("Background thread timed out. Closing all threads")
            except Exception as e:
                logger.error(f"then closing occure {e}")
            os._exit(getattr(os, "_exitcode", 0))
        else:
            logger.info("Background thread finished processing. Closing all threads")
            sys.exit(getattr(os, "_exitcode", 0))
    print("Thread finished, exiting")
    snmp_daemon.shutdown()
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
