def getS3Fs():
    from s3fs import S3FileSystem
    return S3FileSystem(False, 'http://192.168.180.9:8000/', 'GLZG2JTWDFFSCQVE7TSQ', 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw')