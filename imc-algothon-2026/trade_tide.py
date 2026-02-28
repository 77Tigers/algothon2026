from __future__ import annotations

from data_extract.thames_tide import get_thames_fair_price


def fair_price(write_output: bool = False, output_dir: str = ".") -> tuple[int, int]:
    return get_thames_fair_price(write_output=write_output, output_dir=output_dir)


if __name__ == "__main__":
    print(fair_price(write_output=True))
