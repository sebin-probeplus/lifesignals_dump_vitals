import json
import os
import sys
from datetime import datetime

import pandas as pd

from user_config import input_file_path, bcast_file_path, required_vitals, time_interval
from default_config import tb, tf, posture_map


class DumpVitals:
    def __init__(
        self,
    ):
        self.input_filepath = input_file_path
        self.start_time = self.get_start_time(bcast_file_path)
        self.start_time_str = ""
        self.data = []
        self.no_of_lines = 0
        self.req_vitals = required_vitals
        self.t_interval = time_interval
        self.tol_before = (tb / 100) * time_interval
        self.tol_after = (tf / 100) * time_interval
        self.posture_map = posture_map
        self.required_keys = []
        self.extracted_data = {}
        self.patchid = "BXSBX"
        self.final_data = []
        self.main()

    def get_start_time(self, bcast_filepath):
        if os.path.isfile(bcast_filepath):
            try:
                with open(bcast_filepath, "r") as bcast_json:
                    bcast_data = json.load(bcast_json)
                    return bcast_data.get("Capability", {}).get("StartTime", None)
            except json.JSONDecodeError:
                print("Invalid JSON format in the Broadcast file")
                return None
        else:
            print("Invalid Broadcast file path")
            return None

    def epoch_to_str(self, epoch):
        dt = datetime.fromtimestamp(epoch)
        return dt.strftime("%d-%b-%y")

    def epoch_to_time(self, epoch):
        dt = datetime.fromtimestamp(epoch)
        return dt.strftime("%H:%M:%S")

    def validate_start_time(self):
        if not self.start_time:
            print("StartTime Not Found")
            sys.exit(1)
        else:
            self.start_time_str = self.epoch_to_str(self.start_time)

    def load_data(self):
        with open(self.input_filepath, "r") as data_json:
            self.data = [json.loads(line) for line in data_json]

    def get_element(self, arr, index):
        try:
            return arr[index]
        except IndexError:
            return None

    def extract_vitals(self, line_data, key, extracted_data, required_keys):
        value = line_data.get(key)

        if not value or not isinstance(value, list) or len(value) == 0:
            return

        vital_value = value[0]

        if key == "SPO2" or key == "POSTURE" or key == "SKINTEMP":

            if key == "SPO2" and vital_value <= 100:
                extracted_data["SPO2"] = vital_value
                required_keys.remove("SPO2")

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
                        extracted_data["POSTURE"] = mapped_posture
                        required_keys.remove("POSTURE")

            elif key == "SKINTEMP" and vital_value > 0:
                extracted_data["SKINTEMP"] = vital_value / 1000

        else:
            extracted_data[key] = vital_value
            required_keys.remove(key)

    def extract_data_from_line(
        self, before_list, after_list, line_time, reference_time
    ):
        required_keys = self.required_keys.copy()
        extracted_data = {key: "" for key in required_keys}
        while len(required_keys):
            max_len = max(len(before_list), len(after_list))
            for i in range(max_len):
                line_data = self.get_element(before_list, i)
                if line_data:
                    for key in required_keys:
                        self.extract_vitals(
                            line_data, key, extracted_data, required_keys
                        )
                line_data = self.get_element(after_list, i)
                if line_data:
                    for key in required_keys:
                        self.extract_vitals(
                            line_data, key, extracted_data, required_keys
                        )
                if not len(required_keys):
                    break
            break

        row = [
            self.patchid,
            self.epoch_to_str(line_time),
            self.epoch_to_time(reference_time),
        ]
        for key in self.required_keys:
            row.append(extracted_data.get(key, ""))
        self.final_data.append(row)

    def calculate(self):
        reference_time = self.start_time + self.t_interval
        window_time_before = reference_time - self.tol_before
        window_time_after = reference_time + self.tol_after
        before_list = []
        after_list = []
        for line in self.data:
            line_time = line.get("TsECG") / 1e6 + self.start_time
            if window_time_before <= line_time <= window_time_after:
                if line_time < reference_time:
                    before_list.append(line)
                if line_time > reference_time:
                    after_list.append(line)
            if line_time > window_time_after:
                before_list.reverse()
                self.extract_data_from_line(
                    before_list, after_list, line_time, reference_time
                )
                reference_time += self.t_interval
                window_time_before = reference_time - self.tol_before
                window_time_after = reference_time + self.tol_after

    def convert_to_xlsx(self):
        columns = ["Patch_ID", "Date", "Time"]
        columns.extend(self.required_keys)
        df = pd.DataFrame(self.final_data, columns=columns)
        df.to_excel("output.xlsx", index=False, engine="openpyxl")

    def main(self):
        for key, value in required_vitals.items():
            if value:
                self.required_keys.append(key)
                self.extracted_data[key] = ""
        self.validate_start_time()

        print("patch:", self.patchid)
        print("required_vitals:", self.required_keys)
        print("patch_start_time:", self.start_time_str)
        self.load_data()
        print("no_of_lines:", len(self.data))
        print("tolerance_before_in_seconds:", self.tol_before)
        print("tolerance_after_in_seconds:", self.tol_after)
        print("time_interval_in_seconds:", self.t_interval)
        self.calculate()
        self.convert_to_xlsx()


if __name__ == "__main__":
    dumpvitals = DumpVitals()
