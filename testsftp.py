import paramiko
import threading
import boto3
import uuid
import os
from time import sleep
from datetime import datetime
import sys
from awsglue.utils import getResolvedOptions



class FastTransport(paramiko.Transport):
    """Trying to speed things up - probably worth testing whether this is necessary as paramiko improves
    this was on the github report on how to get it to run faster - I had a lot of trouble, but the
    end solution I identified was actually completely different - using the fifo with getfo"""
    
    def __init__(self, sock):
        super(FastTransport, self).__init__(sock)
        self.window_size = 2147483647
        self.packetizer.REKEY_BYTES = pow(2, 40)
        self.packetizer.REKEY_PACKETS = pow(2, 40)

def sftp_connect(host, port, username, pw):
    # https://medium.com/better-programming/transfer-file-from-ftp-server-to-a-s3-bucket-using-python-7f9e51f44e35
    # client = paramiko.SSHClient()
    # client.load_system_host_keys()
    try:
        transport = FastTransport((host, port))
        transport.connect(username=username, password=pw)
        conn = paramiko.SFTPClient.from_transport(transport)
        return conn
    except Exception as e:
        print("Failed on exception", repr(e))
        raise


def sftp_to_s3(host, port, sourcefilename, s3_bucket, destfilename, username, pw):
    print("in sftp_to_s3", host, port, sourcefilename, s3_bucket, destfilename, username)
    conn = sftp_connect(host, port, username, pw)
    with conn.open(sourcefilename, 'rb') as src:
        src_filesize = src._get_size()
    chunk_size = 64 * 1024 * 1024  # 64mb seems about right
    print (src_filesize, "= src_filesize")
    t1 = datetime.now()
    copy_s3_data(conn, sourcefilename, s3_bucket, destfilename, chunk_size, src_filesize)
    t2 = datetime.now()
    print(f"copy took {t2 - t1}")


def write_chunk_to_s3(s3conn, buf, bucket_name, upload_id, s3_destination_path, partnum):
    part = s3conn.upload_part(
        Bucket=bucket_name,
        Key=s3_destination_path,
        PartNumber=partnum,
        UploadId= upload_id,
        Body=buf)
    part_output = {
        'PartNumber': partnum,
        'ETag': part['ETag']
    }
    return part_output

def execute_sink(FIFO, bucket_name, s3_destination_path, chunksize, filesize):
    """Do not start s3 path with a forward slash"""
    chunk = 1
    chunkcount = ((filesize - 1) // chunksize) + 1
    total_transferred = 0
    print("running sink")

    with open(FIFO, 'rb') as fifo:
        
        s3conn = boto3.client('s3')
        uploader = s3conn.create_multipart_upload(Bucket=bucket_name, Key=s3_destination_path)
        print("Got uploader, ", uploader)
        parts = []

        try:
            for chunk in range(chunkcount):
                chunk_transferred = 0
                while chunk_transferred < chunksize and total_transferred < filesize:
                    remaining_chunksize = chunksize - chunk_transferred
                    buf = fifo.read(remaining_chunksize)
                    if not buf:
                        print("oops buf is ", buf)
                        sleep(0.01)
                    this_piece = len(buf)
                    print("len(buf)", this_piece)
                    chunk_transferred = chunk_transferred + this_piece
                    total_transferred = total_transferred + this_piece
                    part = write_chunk_to_s3(s3conn, buf, bucket_name, uploader['UploadId'], s3_destination_path, len(parts) + 1)
                    parts.append(part)

            s3conn.complete_multipart_upload(Bucket=bucket_name,
                                          Key=s3_destination_path,
                                          UploadId=uploader["UploadId"],
                                          MultipartUpload={"Parts": parts})
            print("Done uploading", bucket_name, s3_destination_path, uploader["UploadId"])
            print(parts)

        except:
                s3conn.abort_multipart_upload(Bucket=bucket_name,
                                          Key=s3_destination_path,
                                          UploadId=uploader["UploadId"],
                                          MultipartUpload=parts)
                print("aborting multipart upload", uploader["UploadId"])
                raise
    print ("done with ", chunkcount, "chunks, total filesize = ",filesize,"=", total_transferred)




def execute_source(FIFO, sftpclient, sourcefilename):
    with open(FIFO, 'wb') as fifo:
        sftpclient.getfo(sourcefilename, fifo)

def copy_s3_data(sftpclient, sourcefilename, bucket_name, destfilename, chunksize, filesize ):
    """Copy data through a pipe from sftp server to an s3 bucket

    Be kind of nice to genericise this by providing the ability to just specify a source and dest
    for the data from the fifo.

    """
    FIFO = f"temp_pipe_{uuid.uuid4().hex}"

    def source():
        execute_source(FIFO, sftpclient, sourcefilename)

    def sink():
        execute_sink(FIFO, bucket_name, destfilename, chunksize, filesize)

    os.mkfifo(FIFO)
    try:
        tsrc = threading.Thread(target=source)
        tsrc.start()

        tdest = threading.Thread(target=sink)
        tdest.start()
        print("joining tsrc\n")
        tsrc.join()
        print("joining tdest\n")
        tdest.join()
    finally:
        os.remove(FIFO)



print("runtime name", __name__)


args = getResolvedOptions(sys.argv, ['username', 'pw', 'host', 'port', 'sourcefile', 'destfile', 'destbucket'])
user = args['username']
pw = args['pw']
host = args['host']
port = int(args['port'])
sourcefilename =args['sourcefile']
destfilename =args['destfile']
destbucket = args['destbucket']

sftp_to_s3(host, port, sourcefilename, destbucket, destfilename, user, pw)

print("done with sftp_to_s3")