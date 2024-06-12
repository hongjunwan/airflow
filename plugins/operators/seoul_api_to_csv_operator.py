from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs): # dags에서 가져온 초기값 저장
        super().__init__(**kwargs)
        self.http_conn_id = 'CON_TEST'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context): # call_api 함수 값 반환받아서 파일 생성
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id) # connection id를 통해 connection 정보를 가져올 수 있음 
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame() # 비어있는 dataframe
        start_row = 1
        end_row = 5
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row) # call_api 함수 실행
            total_row_df = pd.concat([total_row_df, row_df]) # 비어있는 dataframe과 위에서 return 받은 dataframe을 합쳐줌
            if len(row_df) < 1000: # row 건수가 1000건이 안되면 탈출
                break
            else: # row 건수가 1000건이면 아래 수행
                start_row = end_row + 1 
                end_row += 1000

        if not os.path.exists(self.path): # directory가 없으면 수행, dag: dags_seoul_api_corona.py 에서 넘겨받은 path 값에 해당하는 폴더가 없으면 생성
            os.system(f'mkdir -p {self.path}') # directory를 만듬
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False) # csv형태로 만듬

    def _call_api(self, base_url, start_row, end_row): # 공공데이터 가져오는 수행부
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers) # requests url 주소로 요청 -> response 변수는 string으로 받음
        contents = json.loads(response.text) # string -> 딕셔너리 형태로 바꿔줌

        key_nm = list(contents.keys())[0] # task log에서 확인 할 수 있는 stationinfo 라는 글자 자체가 들어감
        row_data = contents.get(key_nm).get('row') # task log에서 확인 할 수 있는 row 값을 가져옴
        row_df = pd.DataFrame(row_data) # dataframe로 변경

        return row_df