import AlteryxPythonSDK as Sdk
import json
import boto3
import pandas as pd
import os

class AyxPlugin:
    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        return IncomingInterface(self)

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        Sdk.RecordError().push_error("This tool does not support Push All Records mode.")

    def pi_close(self, b_has_errors: bool):
        self.output_anchor.assert_close()

class IncomingInterface:
    def __init__(self, parent: AyxPlugin):
        self.parent = parent
        self.record_info_in = None

    def ii_init(self, record_info_in: object) -> bool:
        self.record_info_in = record_info_in
        self.record_info_out = record_info_in.clone()

        self.parent.output_anchor.init(self.record_info_out)
        return True

    def ii_push_record(self, in_record: object) -> bool:
        data = {}
        for field in self.record_info_in:
            data[field.name] = field.get_as_string(in_record)

        region = data['AWS Region']

        # Add additional filters and services as needed
        services = [
            ('AmazonRDS', 'Database Instance'),
            ('AmazonEC2', 'Windows Server'),
            ('AmazonEC2', 'Dedicated Host'),
            ('AmazonEC2', 'SQL Server')
        ]

        df_list = []

        for service_code, term_code in services:
            pricing_data = self.parent.get_pricing(service_code, term_code, region)
            df_list.append(pd.json_normalize(pricing_data))

        all_services_pricing = pd.concat(df_list)
        all_services_pricing.reset_index(drop=True, inplace=True)

        out_record = self.record_info_out.construct_record(in_record)

        for field in self.record_info_out:
            if field.name in data:
                field.set_from_string(out_record, data[field.name])

        self.parent.output_anchor.push_record(out_record)
        return True

    def ii_update_progress(self, d_percent: float) -> bool:
        return True

    def ii_close(self) -> bool:
        return
