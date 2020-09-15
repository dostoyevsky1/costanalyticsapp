from datetime import datetime
import base64
import json
import os
from io import StringIO
import re
import logging
import pandas as pd
import azure.functions as func
from azure.identity import DefaultAzureCredential
from msrest.authentication import BasicTokenAuthentication
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline import PipelineRequest, PipelineContext
from azure.core.pipeline.transport import HttpRequest
from plotly import graph_objs as go
import plotly.express as px
import plotly.io as pio

from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

logging.Logger.root.level = 10
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)

class CredentialWrapper(BasicTokenAuthentication):
    def __init__(self, credential=None, resource_id="https://management.azure.com/.default", **kwargs):
        """Wrap any azure-identity credential to work with SDK that needs azure.common.credentials/msrestazure.

        Default resource is ARM (syntax of endpoint v2)

        :param credential: Any azure-identity credential (DefaultAzureCredential by default)
        :param str resource_id: The scope to use to get the token (default ARM)
        """
        super(CredentialWrapper, self).__init__(None)
        if credential is None:
            credential = DefaultAzureCredential()
        self._policy = BearerTokenCredentialPolicy(credential, resource_id, **kwargs)

    def _make_request(self):
        return PipelineRequest(
            HttpRequest(
                "CredentialWrapper",
                "https://fakeurl"
            ),
            PipelineContext(None)
        )

    def set_token(self):
        """Ask the azure-core BearerTokenCredentialPolicy policy to get a token.

        Using the policy gives us for free the caching system of azure-core.
        We could make this code simpler by using private method, but by definition
        I can't assure they will be there forever, so mocking a fake call to the policy
        to extract the token, using 100% public API."""
        request = self._make_request()
        self._policy.on_request(request)
        # Read Authorization, and get the second part after Bearer
        token = request.http_request.headers["Authorization"].split(" ", 1)[1]
        self.token = {"access_token": token}

    def signed_session(self, session=None):
        self.set_token()
        return super(CredentialWrapper, self).signed_session(session)

class AzureHelper:
    def __init__(self):
        pass

    def generate_mgmt_credentials(self):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: generating Azure Management plane credentials via Credential Wrapper for azure identity')
        try:
            self._mgmt_credentials = CredentialWrapper()
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')

    def generate_data_credentials(self):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: generating Azure Data plane credentials via Default Credentials for azure identity')
        try:
            self._data_credentials = DefaultAzureCredential()
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e

    def get_storage_mgmt_client(self, subscription: str):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: acquiring storage resource management client connection')
        self._subscription = subscription
        try:
            self._storage_mgmt_client = StorageManagementClient(self._mgmt_credentials, self._subscription)
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e

    def get_storage_keys(self, resource_group: str, storage_account: str):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: acquiring storage account keys')
        try:
            self._storage_keys = self._storage_mgmt_client.storage_accounts.list_keys(resource_group,storage_account).keys
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e

    def generate_storage_conn_string(self, storage_key: str, storage_account: str):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: acquiring storage account keys')
        try:
            self._storage_conn_string = f'DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_key};EndpointSuffix=core.windows.net'
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e

    def get_storage_client(self, conn_string: str):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: acquiring storage resource client connection')
        if conn_string:
            self._conn_string = conn_string
        try:
            self._storage_client = BlobServiceClient.from_connection_string(self._conn_string, self._data_credentials)
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e
    
    def get_container_client(self, container_name: str):
        logger = logging.getLogger(__name__)
        logger.info('LOGGER: acquiring container client connection')
        self._container_name = container_name
        try:
            self._container_client = self._storage_client.get_container_client(container_name)
        except Exception as e:
            logger.error(f'LOGGER: Exception occurred {e}')
            raise e

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    pio.kaleido.scope.default_height = 1400
    pio.kaleido.scope.default_width = 3200

    els = ['SUB', 'RGROUP', 'STORNAME', 'CNTNR', 'DESTINATION']

    SUB, RGROUP, STORNAME, CNTNR, DESTINATION = [req.params.get(key) for key in els]
    if not all([SUB, RGROUP, STORNAME, CNTNR, DESTINATION]):
        try:
            logger.info('LOGGER: Acquiring request parameters')
            req_body = req.get_json()
        except ValueError:
            logger.error(f'LOGGER: Exception occurred: {ValueError}')
        else:
            SUB, RGROUP, STORNAME, CNTNR, DESTINATION = [req_body.get(key) for key in els]

    TODAY = datetime.today()

    AZ = AzureHelper()
    AZ.generate_data_credentials()
    AZ.generate_mgmt_credentials()
    AZ.get_storage_mgmt_client(SUB)
    AZ.get_storage_keys(RGROUP, STORNAME)
    AZ.generate_storage_conn_string(AZ._storage_keys[0].value, STORNAME)
    AZ.get_storage_client(AZ._storage_conn_string)
    AZ.get_container_client(CNTNR)

    cost_cntnr = AZ._container_client

    all_blobs = [item for item in cost_cntnr.list_blobs()]
    this_month = [item.name for item in all_blobs if datetime.strptime(re.findall('([0-9]{8}-[0-9]{8})',item.name)[0].split('-')[0],'%Y%m%d').month == TODAY.month]

    modified = {}
    for blob in this_month:
        modified[blob] = cost_cntnr.get_blob_client(blob).get_blob_properties().last_modified

    latest_blob = sorted(modified.items(), key = lambda x: x[1], reverse = True)[0][0]
    latest_blob = cost_cntnr.download_blob(latest_blob)
    blob_string = str(latest_blob.content_as_bytes(), 'utf-8')
    blob_string = StringIO(blob_string)

    cost_df = pd.read_csv(blob_string)
    cost_df.columns = [column.lower() for column in cost_df.columns.tolist()]
    cost_df = cost_df.sort_values('date')
    cost_df['total_cost'] = (cost_df['effectiveprice']*cost_df['quantity'])
    cost_df['resourcegroupname'] = [item.lower() for item in cost_df['resourcegroupname']]

    ### PLOT GROUPED BY COST CENTER TAG

    cost_df['tags'] = [json.loads(tag) if isinstance(tag, str) else pd.NA for tag in cost_df['tags']] 
    cost_df['tags'] = [tag.get('CostCenter',pd.NA) if isinstance(tag,dict) else pd.NA for tag in cost_df['tags']]
    cost_df['tags'] = [tag.strip() if isinstance(tag, str) and bool(tag) else 'Other' for tag in cost_df['tags']]
    
    per_day_tags_df = cost_df[['tags','date','total_cost']].groupby(['date','tags']).sum()
    per_day_tags_df = per_day_tags_df.sort_values('total_cost',ascending=False).sort_index(level='date',sort_remaining=False)
    per_day_tags_df = per_day_tags_df.reset_index()
    
    actual_cost = per_day_tags_df['total_cost'].sum()

    if len(per_day_tags_df['date'].unique()) > 1:
        fig = px.area(per_day_tags_df, x = 'date', y = 'total_cost', color = 'tags')
        fig.update_traces(mode='markers+lines')
        fig.update_layout(
            title=f"Daily Resoure Costs for {TODAY.month}/{TODAY.year}",
            xaxis_title="Date",
            yaxis_title="Cost",
            legend_title="Cost Center Tags",
            font=dict(
            size=16,
            family = "Times New Roman"
            ),
            autosize = True,
            hovermode = 'closest',
            legend=dict(
            font = dict(size = 12)
            ),
            annotations = [ 
                dict( 
                    x=3, 
                    y=per_day_tags_df.groupby(['date']).sum().max().total_cost,
                    xref="x", 
                    yref="y", 
                    text=f"Actual Cost: ${actual_cost:.2f}", 
                    font = dict(size=22)
                ) 
            ]
        )
        fig.update_traces(hovertemplate='<br> Total Cost: %{y}')
    else:
        fig = px.scatter(per_day_tags_df, x = 'date', y = 'total_cost', color = 'tags')
        fig.update_traces(mode='markers+lines')
        fig.update_layout(
            title=f"Daily Resoure Costs for {TODAY.month}/{TODAY.year}",
            xaxis_title="Date",
            yaxis_title="Cost",
            legend_title="Cost Center Tags",
            font=dict(
            size=16,
            family = "Times New Roman"
            ),
            autosize = True,
            hovermode = 'x unified',
            legend=dict(
            font = dict(size = 12)
            ),
            annotations = [ 
                dict( 
                    x=0, 
                    y=per_day_tags_df['total_cost'].max() + 10,
                    xref="x", 
                    yref="y", 
                    text=f"Actual Cost: ${per_day_tags_df['total_cost'].sum():.2f}", 
                    font = dict(size=22) 
                ) 
            ]
        )
        fig.update_traces(hovertemplate='<br> Total Cost: %{y}')

    AZ.get_container_client(DESTINATION)
    destination_cntnr = AZ._container_client

    all_destination_blobs = [item.name for item in destination_cntnr.list_blobs()]

    try:
        img = fig.to_html()
        html_blob_name = f'Cost-Graph-{TODAY.year}-{TODAY.month}.html'
        if html_blob_name in all_destination_blobs:
            destination_cntnr.delete_blob(html_blob_name)
        destination_cntnr.upload_blob(html_blob_name, img)
        logger.info(f'LOGGER: Generated HTML image and uploaded {html_blob_name} to {RGROUP}/{STORNAME}/{CNTNR}')
    except Exception as e:
        logger.error(f'LOGGER: Exception occurred: {e}')
        raise e

    try:
        img = fig.to_image(format='png', engine = 'kaleido')
        png_blob_name = f'Cost-Graph-{TODAY.year}-{TODAY.month}.png'
        if png_blob_name in all_destination_blobs:
            destination_cntnr.delete_blob(png_blob_name)
        destination_cntnr.upload_blob(png_blob_name, img)
        logger.info(f'LOGGER: Generated static image and uploaded {png_blob_name} to {RGROUP}/{STORNAME}/{CNTNR}')
    except Exception as e:
        logger.error(f'LOGGER: Exception occurred: {e}')
        raise e

    return func.HttpResponse(json.dumps({'html_blob_name':html_blob_name,'png_blob_name':png_blob_name, 'actual_cost': actual_cost}), status_code = 200)