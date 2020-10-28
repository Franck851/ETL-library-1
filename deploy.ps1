$personalToken = ""
$localPath = "C:\Users\PARJ010\Documents\ETL_namespace\dist\ETL_lib-1.1-py3-none-any.whl"
$remotePath = "dbfs:/mnt/etl_lib/ETL_lib-1.1-py3-none-any.whl"
$clusterId = ""
$databricksInstance = ""

$headers = @{"Authorization" = "Bearer $($personalToken)"}
$jsonRequest = @{libraries=@(@{whl=$remotePath}); cluster_id=$clusterId} | ConvertTo-Json

$fileContent = get-content -Raw $localPath
$fileContentBytes = [System.Text.Encoding]::Default.GetBytes($fileContent)
$fileContentEncoded = [System.Convert]::ToBase64String($fileContentBytes)

[Reflection.Assembly]::LoadWithPartialName("System.Web") | Out-Null
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# Uninstall ETL-lib
Invoke-RestMethod -Method Post -Uri "$databricksInstance/api/2.0/libraries/uninstall" -Headers $headers -body $jsonRequest

# Delete DBFS file
Invoke-RestMethod -Method Post -Uri "$databricksInstance/api/2.0/dbfs/delete" -Headers $headers -Body (@{path=$remotePath} | ConvertTo-Json)

# Upload file to DBFS
Invoke-RestMethod -Method Post -Uri "$databricksInstance/api/2.0/dbfs/put" -Headers $headers -Body (@{path=$remotePath; contents=$fileContentEncoded} | ConvertTo-Json)

# Install ETL-lib
Invoke-RestMethod -Method Post -Uri "$databricksInstance/api/2.0/libraries/install" -Headers $headers -body $jsonRequest

# Restart cluster
Invoke-RestMethod -Method Post -Uri "$databricksInstance/api/2.0/clusters/restart" -Headers $headers -body $jsonRequest