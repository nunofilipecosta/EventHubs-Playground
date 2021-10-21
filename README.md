# Getting started

## Portal

## Azure CLI

1. Configure variables

```console
az configure --defaults group=learn-0d2c0f27-5e67-4e53-8690-4017222d9f4d location=westus2

NS_NAME=ehubns-$RANDOM
```
2. Create the namespace

```console
az eventhubs namespace create --name $NS_NAME
```

3. Get the connection string details

```console
az eventhubs namespace authorization-rule keys list \
    --name RootManageSharedAccessKey \
    --namespace-name $NS_NAME
```
4. Create the event hub

```console
HUB_NAME=hubname-$RANDOM

az eventhubs eventhub create --name $HUB_NAME --namespace-name $NS_NAME

az eventhubs eventhub show --namespace-name $NS_NAME --name $HUB_NAME
```

5. Create a storage account

```console
STORAGE_NAME=storagename$RANDOM

az storage account create --name $STORAGE_NAME --sku Standard_RAGRS --encryption-service blob

az storage account keys list --account-name $STORAGE_NAME

az storage account show-connection-string -n $STORAGE_NAME

az storage container create --name messages --connection-string "<connection string here>"



```





