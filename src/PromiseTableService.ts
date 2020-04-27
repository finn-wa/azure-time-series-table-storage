import {
  common,
  createTableService,
  ServiceResponse,
  StorageHost,
  TableBatch,
  TableQuery,
  TableService,
} from 'azure-storage';

/**
 * Wrapper which promisifies the "legacy" azure-storage TableService, which is due to be
 * replaced any day now... https://github.com/Azure/azure-storage-js/issues/45
 */
export default class PromiseTableService {
  private svc: TableService;

  /**
   * Constructs a new PromiseTableService. Pass null to the constructor leave the
   * internal TableService object uninitialised. You must then set it yourself with
   * setTableService before calling any other methods.
   *
   * If no storageaccount or storageaccesskey are provided, the
   * AZURE_STORAGE_CONNECTION_STRING and then the AZURE_STORAGE_ACCOUNT and
   * AZURE_STORAGE_ACCESS_KEY environment variables will be used.
   * @param storageAccountOrConnectionString The storage account or the connection string.
   * @param storageAccessKey The storage access key.
   * @param host The host address. To define primary only, pass a string. Otherwise
   * 'host.primaryHost' defines the primary host and 'host.secondaryHost' defines the
   * secondary host.
   * @example
   * new PromiseTableService(null)            // does not initialise TableService
   * new PromiseTableService()                // calls createTableService()
   * new PromiseTableService("str")           // calls createTableService("str")
   * new PromiseTableService(str, key, host)  // calls createTableService(str, key, host)
   */
  constructor(connectionString?: string, storageAccessKey?: string, host?: StorageHost) {
    if (connectionString && storageAccessKey) {
      this.svc = createTableService(connectionString, storageAccessKey, host);
    } else if (connectionString) {
      this.svc = createTableService(connectionString);
    } else if (connectionString === undefined) {
      this.svc = createTableService();
    }
  }

  /**
   * Sets the internal TableService object used by the wrapper.
   * @param tableService The TableService to use
   */
  public setTableService(tableService: TableService) {
    this.svc = tableService;
  }

  /**
   * Associate a filtering operation with this TableService. Filtering operations
   * can include logging, automatically retrying, etc. Filter operations are objects
   * that implement a method with the signature: "function handle (requestOptions, next)".
   *
   * After doing its preprocessing on the request options, the method needs to call
   * "next" passing a callback with the following signature:
   * signature: "function (returnObject, finalCallback, next)"
   *
   * In this callback, and after processing the returnObject (the response from the
   * request to the server), the callback needs to either invoke next if it exists to
   * continue processing other filters or simply invoke finalCallback otherwise to end
   * up the service invocation.
   *
   * @param filter The new filter object.
   * @returns The client with the filter applied.
   */
  public withFilter(newFilter: common.filters.IFilter): PromiseTableService {
    this.svc = this.svc.withFilter(newFilter);
    return this;
  }

  /**
   * Gets the service stats for a storage account’s Table service.
   * @param options The request options.
   * @returns The service stats
   */
  public getServiceStats(options?: common.RequestOptions): Promise<ResultResponse<common.models.ServiceStats>> {
    return this.promiseResult<common.models.ServiceStats>(this.svc.getServiceStats, options);
  }

  /**
   * Gets the properties of a storage account’s Table service, including Azure Storage
   * Analytics.
   * @param options The request options.
   * @returns The service properties
   */
  public getServiceProperties(
    options?: common.RequestOptions
  ): Promise<ResultResponse<common.models.ServicePropertiesResult.ServiceProperties>> {
    return this.promiseResult<common.models.ServicePropertiesResult.ServiceProperties>(
      this.svc.getServiceProperties,
      options
    );
  }

  /**
   * Sets the properties of a storage account’s Table service, including Azure Storage
   * Analytics. You can also use this operation to set the default request version for
   * all incoming requests that do not have a version specified.
   * @param serviceProperties The service properties.
   * @param options The request options.
   * @returns The service response
   */
  public setServiceProperties(
    serviceProperties: common.models.ServicePropertiesResult.ServiceProperties,
    options?: common.RequestOptions
  ): Promise<ServiceResponse> {
    return this.promiseResponse(this.svc.setServiceProperties, serviceProperties, options);
  }

  /**
   * Lists a segment containing a collection of table items under the specified account.
   * @param currentToken A continuation token returned by a previous listing operation.
   * Please use 'null' or 'undefined' if this is the first operation.
   * @param options The request options.
   * @returns a list of tables and the `continuationToken` for the next listing operation.
   */
  public listTablesSegmented(
    currentToken: TableService.ListTablesContinuationToken,
    options?: TableService.ListTablesRequestOptions
  ): Promise<ResultResponse<TableService.ListTablesResponse>> {
    return this.promiseResult<TableService.ListTablesResponse>(this.svc.listTablesSegmented, currentToken, options);
  }

  /**
   * Lists a segment containing a collection of table items under the specified account.
   * @param currentToken A continuation token returned by a previous listing operation.
   * Please use 'null' or 'undefined' if this is the first operation.
   * @param options The request options.
   * @returns a list of tables and the `continuationToken` for the next listing operation.
   */
  public listTablesSegmentedWithPrefix(
    prefix: string,
    currentToken: TableService.ListTablesContinuationToken,
    options?: TableService.ListTablesRequestOptions
  ): Promise<ResultResponse<TableService.ListTablesResponse>> {
    return this.promiseResult<TableService.ListTablesResponse>(
      this.svc.listTablesSegmentedWithPrefix,
      prefix,
      currentToken,
      options
    );
  }

  /**
   * Gets the table's ACL.
   * @param table The table name.
   * @param options The request options.
   * @returns The table's ACL.
   */
  public getTableAcl(
    table: string,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.GetTableAclResult>> {
    return this.promiseResult<TableService.GetTableAclResult>(this.svc.getTableAcl, table, options);
  }

  /**
   * Updates the table's ACL.
   * @param table The table name.
   * @param signedIdentifiers The signed identifiers in an array.
   * @param options The request options.
   * @returns Information for the table.
   */
  public setTableAcl(
    table: string,
    signedIdentifiers: { [key: string]: common.AccessPolicy },
    options?: common.RequestOptions
  ): Promise<ResultResponse<{ TableName: string; signedIdentifiers: { [key: string]: common.AccessPolicy } }>> {
    return this.promiseResult<{
      TableName: string;
      signedIdentifiers: { [key: string]: common.AccessPolicy };
    }>(this.svc.setTableAcl, table, signedIdentifiers, options);
  }

  /**
   * Retrieves a shared access signature token.
   * @param table The table name.
   * @param sharedAccessPolicy The shared access policy
   * @returns The shared access signature
   */
  public generateSharedAccessSignature(
    table: string,
    sharedAccessPolicy: TableService.TableSharedAccessPolicy
  ): string {
    return this.svc.generateSharedAccessSignature(table, sharedAccessPolicy);
  }

  /**
   * Retrieves a shared access signature token.
   * @param table The table name.
   * @param sharedAccessPolicy The shared access policy
   * @param sasVersion An optional string indicating the desired SAS version to use.
   * Value must be 2012-02-12 or later.
   * @returns The shared access signature
   */
  public generateSharedAccessSignatureWithVersion(
    table: string,
    sharedAccessPolicy: TableService.TableSharedAccessPolicy,
    sasVersion: string
  ): string {
    return this.svc.generateSharedAccessSignatureWithVersion(table, sharedAccessPolicy, sasVersion);
  }

  /**
   * Checks whether or not a table exists on the service.
   * @param table The table name.
   * @param options The request options.
   * @returns TableResult containing `exists` boolean
   */
  public doesTableExist(
    table: string,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.TableResult>> {
    return this.promiseResult<TableService.TableResult>(this.svc.doesTableExist, table, options);
  }

  /**
   * Creates a new table within a storage account if it does not exist.
   * @param table The table name.
   * @param options The request options.
   * @returns The new table information.
   */
  public createTable(
    table: string,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.TableResult>> {
    return this.promiseResult<TableService.TableResult>(this.svc.createTable, table, options);
  }

  /**
   * Creates a new table within a storage account.
   * @param table The table name.
   * @param options The request options.
   * @returns The new table information.
   */
  public createTableIfNotExists(
    table: string,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.TableResult>> {
    return this.promiseResult<TableService.TableResult>(this.svc.createTableIfNotExists, table, options);
  }

  /**
   * Deletes a table from a storage account.
   * @param table The table name.
   * @param options The request options.
   * @returns Information about this operation.
   */
  public deleteTable(
    serviceProperties: common.models.ServicePropertiesResult.ServiceProperties,
    options?: common.RequestOptions
  ): Promise<ServiceResponse> {
    return this.promiseResponse(this.svc.deleteTable, serviceProperties, options);
  }

  /**
   * Deletes a table from a storage account, if it exists.
   * @param table The table name.
   * @param options The request options.
   * @returns True if table was deleted, false otherwise
   */
  public deleteTableIfExists(table: string, options?: common.RequestOptions): Promise<ResultResponse<boolean>> {
    return this.promiseResult<boolean>(this.svc.createTable, table, options);
  }

  /**
   * Queries data in a table. To retrieve a single entity by partition key and row key,
   * use retrieve entity.
   * @param table The table name.
   * @param tableQuery The query to perform. Use null, undefined, or new TableQuery() to
   * get all of the entities in the table.
   * @param currentToken A continuation token returned by a previous listing operation.
   * Please use 'null' or 'undefined' if this is the first operation.
   * @returns The matching entities. If more matching entities exist, and could not be
   * returned, queryContinuationToken will contain a continuation token that can be used
   * to retrieve the next set of results.
   */
  public queryEntities<T>(
    table: string,
    tableQuery: TableQuery,
    currentToken: TableService.TableContinuationToken,
    options?: TableService.TableEntityRequestOptions
  ): Promise<ResultResponse<TableService.QueryEntitiesResult<T>>> {
    return this.promiseResult<TableService.QueryEntitiesResult<T>>(
      this.svc.queryEntities,
      table,
      tableQuery,
      currentToken,
      options
    );
  }

  /**
   * Retrieves an entity from a table.
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The extended request options, including property and entity resolvers
   * which will be applied if echoContent is set to true.
   * @returns The entity.
   */
  public retrieveEntity<T>(
    table: string,
    partitionKey: string,
    rowKey: string,
    options?: TableService.TableEntityRequestOptions
  ): Promise<ResultResponse<T>> {
    return this.promiseResult<T>(this.svc.retrieveEntity, table, partitionKey, rowKey, options);
  }

  /**
   * Inserts a new entity into a table.
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The extended request options, including property and entity resolvers
   * which will be applied if echoContent is set to true.
   * @returns The entity information.
   */
  public insertEntityWithExtendedOptions<T>(
    table: string,
    entityDescriptor: T,
    options: TableService.InsertEntityRequestOptions
  ): Promise<ResultResponse<T | TableService.EntityMetadata>> {
    return this.promiseResult<T | TableService.EntityMetadata>(this.svc.insertEntity, table, entityDescriptor, options);
  }

  /**
   * Inserts a new entity into a table.
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   * @example
   * const task1 = {
   *   PartitionKey : {'_': 'tasksSeattle', '$':'Edm.String'},
   *   RowKey: {'_': '1', '$':'Edm.String'},
   *   Description: {'_': 'Take out the trash', '$':'Edm.String'},
   *   DueDate: {'_': new Date(2011, 12, 14, 12), '$':'Edm.DateTime'}
   * };
   * await tableService.insertEntity('tasktable', task1);
   */
  public insertEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(this.svc.insertEntity, table, entityDescriptor, options);
  }

  /**
   * Inserts or updates a new entity into a table.
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   */
  public insertOrReplaceEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(
      this.svc.insertOrReplaceEntity,
      table,
      entityDescriptor,
      options
    );
  }

  /**
   * Replaces an existing entity within a table. To replace conditionally based on etag,
   * set entity['.metadata']['etag'].
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   */
  public replaceEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(this.svc.replaceEntity, table, entityDescriptor, options);
  }

  /**
   * Updates an existing entity within a table by merging new property values into the
   * entity. To merge conditionally based on etag, set entity['.metadata']['etag'].
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   */
  public mergeEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(this.svc.mergeEntity, table, entityDescriptor, options);
  }

  /**
   * Inserts or updates an existing entity within a table by merging new property values
   * into the entity.
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   */
  public insertOrMergeEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(
      this.svc.insertOrMergeEntity,
      table,
      entityDescriptor,
      options
    );
  }

  /**
   * Deletes an entity within a table. To delete conditionally based on etag, set entity
   * ['.metadata']['etag'].
   * @param table The table name.
   * @param entityDescriptor The entity descriptor.
   * @param options The request options.
   * @returns The entity information.
   */
  public deleteEntity<T>(
    table: string,
    entityDescriptor: T,
    options?: common.RequestOptions
  ): Promise<ResultResponse<TableService.EntityMetadata>> {
    return this.promiseResult<TableService.EntityMetadata>(this.svc.deleteEntity, table, entityDescriptor, options);
  }

  /**
   *  Executes the operations in the batch.
   * @param table The table name.
   * @param batch The table batch to execute.
   * @param options The request options.
   * @returns Responses for each operation executed in the batch.
   */
  public executeBatch(
    table: string,
    batch: TableBatch,
    options?: TableService.TableEntityRequestOptions
  ): Promise<ResultResponse<TableService.BatchResult[]>> {
    return this.promiseResult(this.svc.executeBatch, table, batch, options);
  }

  public getUrl(table: string, sasToken?: string, primary?: boolean): string {
    return this.svc.getUrl(table, sasToken, primary);
  }

  /**
   * Wraps a ErrorOrResponse callback function in a promise.
   * @param fn Function with an ErrorOrResponse callback.
   * @param args Arguments for the function (minus the callback).
   * @returns A promise containing the ServiceResponse (or an error).
   */
  private promiseResponse(fn: (...params: any[]) => void, ...args: any[]): Promise<ServiceResponse> {
    return new Promise((resolve, reject) => {
      const promiseHandling = (err: Error, response: ServiceResponse) => {
        err ? reject(err) : resolve(response);
      };
      args.push(promiseHandling);
      fn.apply(this.svc, args);
    });
  }

  /**
   * Wraps a ErrorOrResult callback function in a promise.
   * @param fn Function with an ErrorOrResult callback.
   * @param args Arguments for the function (minus the callback).
   * @returns A promise containing the ServiceResponse and the result (or an error).
   */
  private promiseResult<TResult>(fn: (...params: any[]) => void, ...args: any[]): Promise<ResultResponse<TResult>> {
    return new Promise((resolve, reject) => {
      const promiseHandling = (err: Error, result: TResult, response: ServiceResponse) => {
        err ? reject(err) : resolve({ result, response });
      };
      args.push(promiseHandling);
      fn.apply(this.svc, args);
    });
  }
}

export interface ResultResponse<TResult> {
  result: TResult;
  response: ServiceResponse;
}
