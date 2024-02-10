#get the file name from the adf
fileName = dbutils.widgets.get('fileName')
fileNameWithoutExt = fileName.split('.')[0]
print(fileNameWithoutExt)

import pyspark.sql.functions as F


#Just change all the values here based on the resource name you have created in your environemnt and workspace.

stgAccountSASTokenKey = ''
landingFileName =fileName 
databricksScopeName =''
storageContainer =''
storageAccount=''
landingMountPoint ='/mnt'


if not any(mount.mountPoint == landingMountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount), mount_point= landingMountPoint, extra_configs ={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer,storageAccount):dbutils.secrets.get(scope = databricksScopeName, key= stgAccountSASTokenKey)})
    print('Mounted the storage account successfully')
else:
    print('Storage account already mounted')



df1 = spark.read.csv('/mnt/landing/'+fileName, inferSchema=True, header=True)
display(df1)

# Rule
errorFlag=False
errorMessage = ''
totalcount = df1.count()
print(totalcount)
distinctCount = df1.distinct().count()
print(distinctCount)
if distinctCount !=totalcount:
    errorFlag = True
    errorMessage = 'Duplication Found. Rule 1 Failed'
print(errorMessage)
    

if errorFlag:
    dbutils.fs.mv('/mnt/landing/'+fileName,'/mnt/rejected/'+fileName )
    dbutils.notebook.exit('{"errorFlag": "true", "errorMessage":"'+errorMessage +'"}')
else:
    dbutils.fs.mv('/mnt/landing/'+fileName,'/mnt/staging/'+fileName )
    dbutils.notebook.exit('{"errorFlag": "false", "errorMessage":"No error"}')