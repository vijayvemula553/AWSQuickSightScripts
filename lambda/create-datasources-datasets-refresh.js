const AWS = require("aws-sdk");
const fs = require("fs");
var quicksight = new AWS.QuickSight({});
const ddbTable = "storeDataSetIds";

AWS.config.apiVersions = {
    quicksight: "2018-04-01",
    dynamodb: "2012-08-10",
};

const data = fs.readFileSync("dataset.json");
var sources = JSON.parse(data);

const callback = (err, response) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log("Created ResourceName:" + response);
};

exports.handler = async (event, context, cal) => {
    if (!sources) {
        sources = event;
    }
    console.log("REQUEST RECEIVED:\n" + JSON.stringify(sources));
    const invokedFunctionArn = context.invokedFunctionArn;
    const accountId = invokedFunctionArn.split(":")[4];
    try {
        await subscribeQuickSight(quicksight, accountId);
        if(process.env.IDENTITY_TYPE === 'IAM') {
            await createQuickSightUser(quicksight, accountId);
        }
        for (let source of sources["Sources"]) {
            try {
                await createDataSource(quicksight, source, accountId, context)
                await createDataSet(quicksight, source, accountId, context);
                await writeMetadataToDDB(source, context, context)
                if (source.type === 'INCREMENTAL_REFRESH') {

                    await putDataSetProperties(quicksight, accountId, source, context);
                }
            } catch (err) {
                console.error(err);
            }
        }
    } catch (err) {
        console.log(err)
    }

};


const createDataSource = (quicksight, source, accountId, context, cal) => {
    var params = {
        AwsAccountId: accountId,
        DataSourceId: source.dataSourceName,
        Name: source.dataSourceName,
        Type: source.dataSource,
        Credentials: dataSourceCredentials(source),
        DataSourceParameters: getDataSourceParameters(source, context),
    };

    if(process.env.IDENTITY_TYPE === 'IAM') {
        params.Permissions = [
            {
                Principal: process.env.QUICKSIGHT_IAM_USER,
                Actions: ['quicksight:UpdateDataSourcePermissions',
                    'quicksight:DescribeDataSourcePermissions',
                    'quicksight:PassDataSource',
                    'quicksight:DescribeDataSource',
                    'quicksight:DeleteDataSource',
                    'quicksight:UpdateDataSource'
                ],
            }
        ]
        }


    return new Promise((res, rej) => {
        quicksight.createDataSource(params, (err, data) => {
            if (err) {
                if (err.code === 'ResourceExistsException') {
                    console.warn('Warn:', err.message);
                    res()
                } else {
                    console.error('Error:', err.message);
                    rej('Creating data source')
                }
            } else {
                console.log(
                    `Created data source ${source.dataSourceName} with ID ${source.dataSourceName}`
                );
                res()
            }
        });
    })
};

const createDataSet = (quicksight, source, accountId, context, cal) => {

    const params = {
        AwsAccountId: accountId,
        DataSetId: source.dataSetName,
        ImportMode: "SPICE",
        Name: source.dataSetName,
        PhysicalTableMap: getPhysicalTableMap(source, accountId),
    };

    if(process.env.IDENTITY_TYPE === 'IAM') {
        params.Permissions = [
            {
                Principal: process.env.QUICKSIGHT_IAM_USER,
                Actions: ['quicksight:PassDataSet',
                    'quicksight:DescribeIngestion',
                    'quicksight:CreateIngestion',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:DescribeDataSet',
                    'quicksight:CancelIngestion',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:ListIngestions',
                    'quicksight:UpdateDataSetPermissions'
                ],
            }
        ]
    }

    return new Promise((res, rej) => {
        quicksight.createDataSet(params, (err, data) => {
            if (err) {
                if (err.code === 'ResourceExistsException') {
                    console.warn('Warn:', err.message);
                    res()
                } else {
                    console.error('Error:', err.message);
                    rej('Error creating data set')
                }
            } else {
                console.log(`Created data set ${source.dataSetName} with ID ${source.dataSetName}`);
                res()
            }
        });
    })


};

const writeMetadataToDDB = (source, context, cal) => {

    const params = {
        TableName: ddbTable,
        Key: {
            'dataSource': {S: source.dataSource},
            'type': {S: source.type}
        }
    };
    return new Promise((res, rej) => {
        new AWS.DynamoDB().getItem(params, function (err, data) {
            if (err) {
                console.log("Error", err);
                rej(err)
            } else {
                if (data.Item) {
                    console.log("warn: error writing to ddb. item exists ", data.Item);
                    res()
                } else {
                    new AWS.DynamoDB().putItem({
                        "TableName": ddbTable,
                        "Item": {
                            dataSource: {S: source.dataSource},
                            type: {S: source.type},
                            description: {S: source.dataSetName},
                            pdsId: {S: source.dataSetName},
                            sourceType: {S: source.sourceType},
                        }
                    }, function (err, data) {
                        if (err) {
                            console.log('Error putting item into dynamodb failed: ' + err.message);
                            rej(err)
                        } else {
                            console.log('Success writing data: ' + JSON.stringify(source.dataSetName, null, '  '));
                            res()
                        }
                    });

                }
            }
        });
    })

};

const getDataSourceParameters = (source, context) => {
    const dataSourceParameters = {};

    const dataSource = source.dataSource.toLowerCase().trim();

    if (dataSource === 'amazonelasticsearch') {
        dataSourceParameters.AmazonElasticsearchParameters = {
            Domain: source.domain,
        };
    } else if (dataSource === 'amazonopensearch') {
        dataSourceParameters.AmazonOpenSearchParameters = {
            Domain: source.domain,
        };
    } else if (dataSource === 'athena') {
        dataSourceParameters.AthenaParameters = {
            RoleArn: context.invokedFunctionArn,
            WorkGroup: source.workGroup,
        };
    } else if (dataSource === 'aurora') {
        dataSourceParameters.AuroraParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'aurorapostgresql') {
        dataSourceParameters.AuroraPostgreSqlParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'awsiotanalytics') {
        dataSourceParameters.AwsIotAnalyticsParameters = {
            DataSetName: source.dataSetName,
        };
    } else if (dataSource === 'databricks') {
        dataSourceParameters.DatabricksParameters = {
            Host: source.host,
            Port: source.port,
            SqlEndpointPath: source.sqlEndPointPath,
        };
    } else if (dataSource === 'exasol') {
        dataSourceParameters.ExasolParameters = {
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'jira') {
        dataSourceParameters.JiraParameters = {
            SiteBaseUrl: source.siteBaseUrl,
        };
    } else if (dataSource === 'mariadb') {
        dataSourceParameters.MariaDbParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'mysql') {
        dataSourceParameters.MySqlParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'oracle') {
        dataSourceParameters.OracleParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'postgresql') {
        dataSourceParameters.PostgreSqlParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'presto') {
        dataSourceParameters.PrestoParameters = {
            Catalog: source.catalog,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'rds') {
        dataSourceParameters.RdsParameters = {
            Database: source.database,
            InstanceId: source.instanceId,
        };
    } else if (dataSource === 'redshift') {
        dataSourceParameters.RedshiftParameters = {
            Database: source.database,
            ClusterId: source.clusterId,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 's3') {
        dataSourceParameters.S3Parameters = {
            ManifestFileLocation: {
                Bucket: source.bucket,
                Key: source.key,
            },
            RoleArn: source.roleArn,
        };
    } else if (dataSource === 'servicenow') {
        dataSourceParameters.ServiceNowParameters = {
            SiteBaseUrl: source.siteBaseUrl,
        };
    } else if (dataSource === 'snowflake') {
        dataSourceParameters.SnowflakeParameters = {
            Database: source.database,
            Host: source.host,
            Warehouse: source.wareHouse,
        };
    } else if (dataSource === 'spark') {
        dataSourceParameters.SparkParameters = {
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'sqlserver') {
        dataSourceParameters.SqlServerParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'teradata') {
        dataSourceParameters.TeradataParameters = {
            Database: source.database,
            Host: source.host,
            Port: source.port,
        };
    } else if (dataSource === 'twitter') {
        dataSourceParameters.TwitterParameters = {
            MaxRows: source.number.maxRows,
            Query: source.query,
        };
    }

    return dataSourceParameters;
};

const dataSourceCredentials = (source) => {
    const credentials = {
        CredentialPair: {},
    };
    if (source.secretManagerArn) {
        credentials.CredentialPair.SecretManagerArn = source.secretManagerArn;
    } else if (source.userName && source.password) {
        credentials.CredentialPair.Password = source.password;
        credentials.CredentialPair.Username = source.userName;
    }
    return credentials;
};


function getPhysicalTableMap(source, accountId) {
    const dataSourceArn = 'arn:aws:quicksight:' + process.env.AWS_REGION + ':' + accountId + ':datasource/' + source.dataSetName;

    let physicalTableMap = {};
    if (source.type === 'CustomSql') {
        physicalTableMap = {
            CustomSql: {
                DataSourceArn: dataSourceArn,
                Name: source.dataSetName,
                SqlQuery: source.sql,
                // [{
                // "Name": 'year',
                //  "Type": 'INTEGER'
                //  },
                // {
                //  Name": 'fl_date',
                // "Type": 'DATETIME'
                // }
                // ],
                Columns: source.columns,
            },
        };
    } else if (source.type === 'S3Source') {
        physicalTableMap = {
            S3Source: {
                DataSourceArn: dataSourceArn,
                InputColumns: source.columns,
                //{
                //ContainsHeader: true,
                //Delimiter: ';',
                //Format: 'CSV',
                //StartFromRow: '1',
                //TextQualifier: 'DOUBLE_QUOTE',
                //},
                UploadSettings: source.UploadSettings,
            },
        };
    } else {
        physicalTableMap = {
            RelationalTable: {
                DataSourceArn: dataSourceArn,
                InputColumns: source.columns,
                Name: source.dataSetName,
                Catalog: source.dataSetName,
                Schema: source.dataSetName,
            },
        };
    }

    const finalPhysicalTableMap = {
        [source.table]: physicalTableMap,
    };

    return finalPhysicalTableMap;
}

function createQuickSightUser(quicksight, accountId) {

    var params = {
        AwsAccountId: accountId,
        Email: process.env.USER_EMAIL,
        IdentityType: process.env.IDENTITY_TYPE,
        Namespace: 'default',
        UserRole: 'ADMIN',
        IamArn: process.env.QUICKSIGHT_IAM_USER
    }
    return new Promise((resolve, reject) => {
        new AWS.QuickSight({region: process.env.QUICKSIGHT_IAM_REGION}).registerUser(params, (err, data) => {
            if (err) {
                if (err.code === 'ResourceExistsException') {
                    console.warn('Warn:', err.message);
                    resolve()
                } else {
                    console.error('Error:', err.message);
                    reject('error createQuickSightUser')
                }
            } else {
                console.log(
                    `createQuickSightUser Successful`
                );
                resolve()
            }
        });
    })

}


function subscribeQuickSight(quicksight, accountId) {
    var params = {
        AwsAccountId: accountId,
        AccountName: process.env.ACCOUNT_NAME,
        AuthenticationMethod: 'IAM_AND_QUICKSIGHT',
        Edition: 'ENTERPRISE',
        EmailAddress: process.env.ACCOUNT_EMAIL_ADDRESS,
        NotificationEmail: process.env.ACCOUNT_EMAIL_ADDRESS
    }
    return new Promise((resolve, reject) => {
        new AWS.QuickSight({region: process.env.QUICKSIGHT_IAM_REGION}).createAccountSubscription(params, (err, data) => {
            if (err) {
                if (err.code === 'ResourceExistsException') {
                    console.warn('Warn:', err.message);
                    resolve()
                } else {
                    console.error('Error:', err.message);
                    reject('Error subscribe to  QuickSight')
                }

            } else {
                console.log(
                    `Subscribe to QuickSight Successful`
                );
                resolve()
            }
        });
    })
}


function putDataSetProperties(quicksight, accountId, source) {
    var params = {
        AwsAccountId: accountId,
        DataSetId: source.dataSetName,
        DataSetRefreshProperties: {
            RefreshConfiguration: {
                IncrementalRefresh: {
                    LookbackWindow: {
                        Size: source.Size,
                        ColumnName: source.ColumnName,
                        SizeUnit: source.SizeUnit
                    }
                }
            }
        }
    }

    return new Promise((resolve, reject) => {
        new AWS.QuickSight({}).putDataSetRefreshProperties(params, (err, data) => {
            if (err) {
                if (err.code === 'ResourceExistsException') {
                    console.warn('Warn:', err.message);
                    resolve()
                } else {
                    console.error('Error:', err.message);
                    reject('error while updating DataSetProperties')
                }

            } else {
                console.log(
                    `updated DataSetProperties`
                );
                resolve()
            }
        });
    })
}
