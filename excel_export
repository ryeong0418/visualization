import os
import pandas as pd

from datetime import datetime

from src import common_module as cm
from src.common.constants import SystemConstants, TableConstants
from src.common.utils import SystemUtils, ExcelUtils


class Visualization(cm.CommonModule):
    """
    export > sql_excel > sql에 있는 query에 따른 데이터들을 엑셀파일로 출력하여
    export > sql_excel > excel에 데이터 저장
    """

    def __init__(self, logger):
        super().__init__(logger=logger)

    def main_process(self):
        """
        Visualization main 함수.
        """
        self.logger.debug("Visualization")
        self._init_sa_target()

        query_folder = self.config["home"] + "/" + SystemConstants.SQL_PATH
        excel_path = self.config["home"] + "/" + SystemConstants.EXCEL_PATH

        SystemUtils.get_filenames_from_path(query_folder)
        SystemUtils.get_filenames_from_path(excel_path)

        sql_name_list = os.listdir(query_folder)
        sql_split = [i.split(" ") for i in sql_name_list]
        sql_split_sort = sorted(sql_split, key=lambda x: (int(x[0].split("-")[0]), int(x[0].split("-")[1])))
        sql_name_list_sort = [" ".join(i) for i in sql_split_sort]

        for sql_name in sql_name_list_sort:
            sql_query = SystemUtils.get_file_content_in_path(query_folder, sql_name)
            table_name = TableConstants.AE_TXN_SQL_SUMMARY
            df = self.st.get_data_by_query_and_once(sql_query, table_name)
            result_df = self.data_processing(df)
            sheet_name_txt = sql_name.split(".")[0]
            now_day = datetime.now()
            now_date = now_day.strftime("%y%m%d")
            sql_number = sheet_name_txt.split(" ")[0].split("-")[1]

            if sql_number == "1" and len(sql_number) == 1:
                excel_file = excel_path + "/" + sheet_name_txt + "_" + now_date + ".xlsx"
                ExcelUtils.excel_export(excel_file, sheet_name_txt, result_df)

            else:
                ExcelUtils.excel_export(excel_file, sheet_name_txt, result_df)

    @staticmethod
    def data_processing(df):
        """
        Visualization 데이터 전처리 함수.
        :param df: 전처리 전 데이터 프레임
        :return: 전처리 후 데이터 프레임
        """
        df.columns = map(lambda x: str(x).upper(), df.columns)
        df = df.apply(pd.to_numeric, errors="ignore")

        if "TIME" in df.columns:
            df["TIME"] = pd.to_datetime(df["TIME"])

        return df
