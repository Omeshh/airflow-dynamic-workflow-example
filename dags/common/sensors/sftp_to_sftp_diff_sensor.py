from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.utils.decorators import apply_defaults
from airflow.models.taskreschedule import TaskReschedule
from airflow.utils import timezone


class SFTPToSFTPDiffCheck(BaseSensorOperator):
    """
    Compares directory contents between source and all destinations.
    Checks, by default, once every five minutes for 30 minutes; after which,
    the job will fail. If poke_interval set too low, scheduler will inflate interval.

    :param source_conn_id:      The connection id for the source SFTP.
    :type source_conn_id:       str
    :param source_path:         The path on the source SFTP server.
    :type source_path:          str
    :param target_full_path:    The connections and paths for the target SFTPs.
    :type target_full_path:     List[Tuple2(str,str)]
    :param filter_function:     Function that is called to filter the file name
                                filter_function(file_name:str):bool
    :type filter_function:      function
    """

    @apply_defaults
    def __init__(self,
                 source_conn_id,
                 source_path,
                 target_full_path,
                 filter_function=None,
                 poke_interval=300,
                 timeout=1800,
                 *args,
                 **kwargs):
        super(SFTPToSFTPDiffCheck, self).__init__(*args, **kwargs)
        self.mode = 'reschedule'
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.source_conn_id = source_conn_id
        self.source_path = source_path
        self.target_full_path = target_full_path
        self.filter_function = filter_function

    def poke(self, context):
        self.log.info(f'Checking for new files between {self.source_conn_id}{self.source_path} and {self.target_full_path}')
        source_hook = SFTPHook(ftp_conn_id=self.source_conn_id)
        source_files = source_hook.list_directory(self.source_path)
        new_files = 0
        for target in self.target_full_path:
            target_connection = target[0]
            target_path = target[1]
            target_hook = SFTPHook(ftp_conn_id=target_connection)
            target_files = target_hook.list_directory(target_path)

            for file in source_files:
                if self.filter_function is None or self.filter_function(file):
                    if file not in target_files:
                        new_files += 1
        if new_files == 0:
            self.log.info(f"No new files detected. Waiting {self.poke_interval} seconds and checking again.")
            return False
        else:
            return True
