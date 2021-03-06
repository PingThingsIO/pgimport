import os
import abc
import boto3

from pgimport.parse import File

class S3File(File):
    def __init__(self, path):
        super().__init__(path)
        self.is_s3 = path.startswith("s3://")
        if self.is_s3:
            self.bucket, self.key = path.split('/',2)[-1].split('/',1)

##########################################################################
## Cloud Provider Mixins
##########################################################################

class CloudMixin(metaclass=abc.ABCMeta):
    """
    CloudMixin is an interface that implements only one method, connect(), 
    which is meant to return a connection to a cloud provider that will allow 
    subclasses to issue commands to the provider to locate and parse files

    Parameters
    ----------
    kwargs: dict
        key/value pairs specifying credentials for cloud provider
    """
    def __init__(self, **kwargs):
        self.params = {k.lower():v for k,v in kwargs.items()}

    @abc.abstractmethod
    def connect(self):
        """
        connect to cloud provider.
        """
        raise NotImplementedError
    
class S3Mixin(CloudMixin):
    """
    Provides connection to s3.

    Parameters
    ----------
    kwargs: dict
        key/value pairs specifying credentials for s3
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def connect(self):
        """
        Uses boto3 to connect to s3

        Returns
        -------
        client: botocore.client.S3
            s3 connection
        """
        key = self.params.pop("aws_access_key_id", None) or os.environ.get("AWS_ACCESS_KEY_ID")
        secret = self.params.pop("aws_secret_access_key", None) or os.environ.get("AWS_SECRET_ACCESS_KEY")
        token = self.params.pop("aws_session_token", None) or os.environ.get("AWS_SESSION_TOKEN")

        session = boto3.Session(
            aws_access_key_id=key,  aws_secret_access_key=secret, aws_session_token=token
        )
        client = session.client("s3")

        # check validity of credentials. will throw a botocore.exceptions.ClientError
        # if credentials are not valid
        sts = boto3.client("sts")
        sts.get_caller_identity()
        return client
    
    def list_objects(self, bucket, prefix):
        """
        Retrieves urls for all objects in provided bucket and subdirectory (aka prefix)

        Parameters
        ----------
        bucket: str
            s3 bucket that contains data files
        prefix: str
            (optional) subdirectory within s3 bucket that contains data files

        Returns
        -------
        list of string urls to s3 objects containing data files
        """
        objs = self.client.list_objects(Bucket=bucket, Prefix=prefix)
        return [f"s3://{bucket}/{obj['Key']}" for obj in objs["Contents"]]