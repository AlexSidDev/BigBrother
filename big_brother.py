import sys
from big_brother.utils import setup_logging


def main():
    setup_logging()


if __name__ == "__main__":
    sys.exit(main() or 0)