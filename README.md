# Azure Table Storage - Time Series Data Test

- Generates fake hourly percentage data and puts it into an Azure Queue
- Locally tests an Azure Function which consumes the queue and puts it into Azure Table Storage
- Also includes a promisified version of the "legacy" (but not yet replaced) azure-storage TableService
- Can be used with the local [Azure Storage Emulator](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator) (also legacy but not yet replaced)
