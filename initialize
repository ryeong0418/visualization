from src import common_module as cm
from src.common.constants import SystemConstants
from src.common.utils import SystemUtils
from sql.common_sql import CommonSql


class Initialize(cm.CommonModule):
    """
    Initialize Class

    통합 분석 모듈 기능 동작 전 Table 생성 및 Meta Data를 저장 하는 Class
    """

    def __init__(self, logger):
        super().__init__(logger=logger)

    def main_process(self):
        self.logger.debug("SaTarget init")

        self._init_sa_target()

        self._create_table()

        self._insert_init_meta()

    def _create_table(self):
        """
        통합 분석 모듈에서 사용하는 Table를 생성 하는 함수.
        /sql/initialize/ddl 하위 모든 파일 (.txt)를 로드하여 DDL 구문을 호출 한다.
        :return:
        """
        analysis_target_type = f"{self.config['analysis_repo']['analysis_target_type']}/"

        self.logger.debug("_create_table() start")
        init_ddl_path = (
            f"{self.sql_file_root_path}"
            f"{SystemConstants.SPA_PATH}"
            f"{analysis_target_type}"
            f"{SystemConstants.DDL_PATH}"
        )
        init_files = SystemUtils.get_filenames_from_path(init_ddl_path)

        for init_file in init_files:
            with open(f"{init_ddl_path}{init_file}", mode="r", encoding="utf-8") as file:
                ddl = file.read()

            self.st.create_table(ddl)

    def _insert_init_meta(self):
        """
        각 분석 타겟에서 사용할 Meta Data를 저장 하는 함수.
        :return:
        """
        self.logger.debug("_insert_init_meta() start")
        if self.config["intermax_repo"]["use"]:
            self._init_im_target()

            self._insert_init_meta_by_target(SystemConstants.WAS_PATH, self.imt)
            self._teardown_im_target()

        if self.config["maxgauge_repo"]["use"]:
            self._init_mg_target()

            self._insert_init_meta_by_target(SystemConstants.DB_PATH, self.mgt)
            self._teardown_mg_target()

    def _insert_init_meta_by_target(self, target, target_instance):
        """
        각 분석 타겟에 해당되는 Meta Data 조회 text를 로드하여 통합 분석 DB에 저장 하는 함수.
        :param target: 분석 타겟 path 구분을 위한 값.
        :param target_instance: 분석 타겟 instance
        :return:
        """
        analysis_target_type = ""

        if target == "db":
            analysis_target_type = f"{self.config['maxgauge_repo']['analysis_target_type']}/"

        init_meta_path = (
            f"{self.sql_file_root_path}" f"{target}" f"{analysis_target_type}" f"{SystemConstants.META_PATH}"
        )
        init_files = SystemUtils.get_filenames_from_path(init_meta_path)

        delete_query = CommonSql.TRUNCATE_TABLE_DEFAULT_QUERY

        for init_file in init_files:
            with open(f"{init_meta_path}{init_file}", mode="r", encoding="utf-8") as file:
                meta_query = file.read()

            target_table_name = SystemUtils.extract_tablename_in_filename(init_file)
            delete_dict = {"table_name": target_table_name}

            self.st.delete_data(delete_query, delete_dict)

            for meta_df in target_instance.get_data_by_query(meta_query):
                self.st.insert_table_by_df(meta_df, target_table_name)
