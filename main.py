import json
import os
import sys
import copy
import pytz
import argparse
from datetime import datetime

import pandas as pd

from user_config import required_vitals
from default_config import tb, tf, posture_map


class DumpVitals:
    def __init__(self, args):
        self.patchid = None
        self.start_time = None
        self.tz = pytz.timezone(args.timezone)
        self.debug = args.debug
        self.input_filepath = args.filepath
        self.get_start_time_and_patchid(args.bcastpath)
        self.start_time_str = ""
        self.data = []
        self.req_vitals = required_vitals
        self.t_interval = args.timeinterval
        self.tol_before = (tb / 100) * args.timeinterval
        self.tol_after = (tf / 100) * args.timeinterval
        self.posture_map = posture_map
        self.required_keys = []
        self.extracted_data = {}
        self.final_data = []
        self.main()

    def convert_to_xlsx(self):
        columns = ["Patch_ID", "Date", "Time"]
        columns.extend(self.required_keys)
        df = pd.DataFrame(self.final_data, columns=columns)
        df.to_excel("output.xlsx", index=False, engine="openpyxl")

    def get_start_time_and_patchid(self, bcast_filepath):
        try:
            with open(bcast_filepath, "r") as bcast_json:
                bcast_data = json.load(bcast_json)
                self.patchid = bcast_data.get("PatchInfo", {}).get("PatchId", None)
                self.start_time = bcast_data.get("Capability", {}).get(
                    "StartTime", None
                )
        except json.JSONDecodeError:
            print("Invalid JSON format in the Broadcast file")
            sys.exit(1)

    def epoch_to_str(self, epoch):
        dt = datetime.fromtimestamp(epoch, self.tz)
        return dt.strftime("%d-%b-%y")

    def epoch_to_time(self, epoch):
        dt = datetime.fromtimestamp(epoch, self.tz)
        return dt.strftime("%H:%M:%S")

    def validate_start_time_and_patchid(self):
        if not self.start_time:
            print("StartTime Not Found")
            sys.exit(1)
        else:
            self.start_time = round(self.start_time // 60) * 60
            self.start_time_str = self.epoch_to_str(self.start_time)

        if not self.patchid:
            print("patchid Not Found")
            sys.exit(1)
        else:
            if len(self.patchid) == 5:
                self.patchid = str(self.patchid).upper()
            else:
                print("Invalid patchid")
                sys.exit(1)

    def extract_vitals(self, line_data, extracted_data, line_time, reference_time):
        time_diff = abs(reference_time - line_time)

        for key in self.required_keys:
            value = line_data.get(key)

            if time_diff < extracted_data[key]["time_diff"]:

                if not value or not isinstance(value, list) or len(value) == 0:
                    continue

                vital_value = value[0]

                if key == "SPO2" or key == "POSTURE" or key == "SKINTEMP":

                    if key == "SPO2" and vital_value <= 100:
                        if self.debug:
                            extracted_data["SPO2"][
                                "value"
                            ] = f"{vital_value}-({line_data.get('TsECG')})"
                        else:
                            extracted_data["SPO2"]["value"] = vital_value

                        extracted_data["SPO2"]["time_diff"] = time_diff

                    elif key == "POSTURE":
                        posture = line_data.get("POSTURE")
                        posture_fine = line_data.get("POSTURE_FINE")

                        if (
                            posture
                            and posture_fine
                            and isinstance(posture, list)
                            and isinstance(posture_fine, list)
                            and len(posture) > 0
                            and len(posture_fine) > 0
                            and posture[0] in [-1, 0, 1, 2, 3]
                            and posture_fine[0] in [0, 1, 2, 3, 4]
                        ):
                            posture_key = f"{posture[0]}{posture_fine[0]}"
                            mapped_posture = self.posture_map.get(posture_key)

                            if mapped_posture:
                                if self.debug:
                                    extracted_data["POSTURE"][
                                        "value"
                                    ] = f"{mapped_posture}-({line_data.get('TsECG')})"
                                else:
                                    extracted_data["POSTURE"]["value"] = mapped_posture

                                extracted_data["POSTURE"]["time_diff"] = time_diff

                    elif key == "SKINTEMP" and vital_value > 0:
                        if len(str(vital_value)) == 4:
                            vital_value = vital_value / 100
                        else:
                            vital_value = vital_value / 1000

                        vital_value = (vital_value * 1.8) + 32
                        vital_value = "{:.3f}".format(vital_value)

                        if self.debug:
                            extracted_data["SKINTEMP"][
                                "value"
                            ] = f"{vital_value}-({line_data.get('TsECG')})"
                        else:
                            extracted_data["SKINTEMP"]["value"] = vital_value
                        extracted_data["SKINTEMP"]["time_diff"] = time_diff

                else:
                    if vital_value > 0:
                        if self.debug:
                            extracted_data[key][
                                "value"
                            ] = f"{vital_value}-({line_data.get('TsECG')})"
                        else:
                            extracted_data[key]["value"] = vital_value
                        extracted_data[key]["time_diff"] = time_diff

    def calculate(self):
        extracted_data = copy.deepcopy(self.extracted_data)
        reference_time = self.start_time + self.t_interval
        window_time_before = reference_time - self.tol_before
        window_time_after = reference_time + self.tol_after
        try:
            with open(self.input_filepath, "r") as data_json:
                for line in data_json:
                    line_data = json.loads(line)
                    line_time = line_data.get("TsECG") / 1e6 + self.start_time
                    if window_time_before <= line_time <= window_time_after:
                        self.extract_vitals(
                            line_data,
                            extracted_data,
                            line_time,
                            reference_time,
                        )
                    if line_time > window_time_after:
                        row = [
                            self.patchid,
                            self.epoch_to_str(line_time),
                            self.epoch_to_time(reference_time),
                        ]
                        for key in self.required_keys:
                            row.append(extracted_data[key]["value"])
                        self.final_data.append(row)
                        extracted_data = copy.deepcopy(self.extracted_data)
                        reference_time += self.t_interval
                        window_time_before = reference_time - self.tol_before
                        window_time_after = reference_time + self.tol_after
        except FileNotFoundError:
            print(f"File not found: {self.input_filepath}")
        except IOError as e:
            print(f"Error reading file: {e}")

    def main(self):
        for key, value in required_vitals.items():
            if value:
                self.required_keys.append(key)
                self.extracted_data[key] = {"value": "", "time_diff": float("inf")}
        self.validate_start_time_and_patchid()

        print("patch:", self.patchid)
        print("required_vitals:", self.required_keys)
        print(
            "patch_start_time:",
            self.start_time_str,
            self.epoch_to_time(self.start_time),
        )
        print("tolerance_before_in_seconds:", self.tol_before)
        print("tolerance_after_in_seconds:", self.tol_after)
        print("time_interval_in_seconds:", self.t_interval)
        self.calculate()
        self.convert_to_xlsx()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the DumpVitals script with arguments."
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    parser.add_argument(
        "--filepath", type=str, required=True, help="Path to the JSON file"
    )

    parser.add_argument(
        "--bcastpath", type=str, required=True, help="Path to the bcast file"
    )

    parser.add_argument(
        "--timezone", type=str, required=False, help="Timezone for parsing epoch"
    )

    parser.add_argument(
        "--timeinterval",
        type=int,
        required=True,
        help="Interval at which valid data is dumped (in seconds)",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.filepath) or not args.filepath.endswith(".json"):
        print("Invalid JSON file path")

    if not os.path.isfile(args.bcastpath) or not args.bcastpath.endswith(".json"):
        print("Invalid bcast file path")

    if args.timeinterval < 1:
        print("Invalid time interval")

    if args.timezone:
        if args.timezone not in pytz.all_timezones:
            print("Invalid timezone")
    else:
        args.timezone = "Australia/Adelaide"

    dumpvitals = DumpVitals(args=args)
