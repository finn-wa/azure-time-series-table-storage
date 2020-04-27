import { DequeuedMessageItem, QueueClient } from '@azure/storage-queue';
import { generateHourlyData } from './DataGenerator';
import PromiseTableService from './PromiseTableService';

main().catch((e) => console.log(e.message));

async function main() {
  const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
  if (!connectionString) {
    throw new Error('No connection string found');
  }
  // Create empty test table
  const tableService = new PromiseTableService(connectionString);
  await tableService.deleteTableIfExists('test');
  await tableService.createTable('test');
  // Connect to queue
  const queueName = 'water-level-queue';
  const queueClient = new QueueClient(connectionString, queueName);
  await queueClient.create();
  await queueClient.clearMessages();

  // Test mock data on queue/table services
  for (const outMsg of generateHourlyData(7)) {
    await queueClient.sendMessage(JSON.stringify(outMsg));
    const receiveMsgs = await queueClient.receiveMessages();
    for (const inMsg of receiveMsgs.receivedMessageItems) {
      await processMessage(inMsg);
    }
  }
  console.log('Done');
}

async function processMessage(packet: DequeuedMessageItem) {
  const msg = JSON.parse(packet.messageText) as TelemetryMessage;
}

export interface TelemetryMessage {
  waterLevel: number;
  date: Date;
}
