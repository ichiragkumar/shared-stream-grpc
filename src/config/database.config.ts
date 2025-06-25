import { MongoClient, Db } from 'mongodb';
import { config } from 'dotenv';

config();

export class DatabaseService {
  private static db: Db;
  private static activeChangeStreams: Set<any> = new Set();
  
  static async connect(): Promise<Db> {
    if (this.db) return this.db;

    const mongoUri = process.env.MONGODB_URI;
    const dbName = process.env.DB_NAME;

    if (!mongoUri || !dbName) {
      throw new Error('Missing MongoDB URI or Database Name in .env');
    }


    
    const client = await MongoClient.connect(mongoUri, {
      maxPoolSize: 5,  // Reduced from 10 to 5
      minPoolSize: 1,  // Reduced from 3 to 1
      maxIdleTimeMS: 15000, // Close idle connections after 15 seconds (reduced from 30)
      waitQueueTimeoutMS: 10000, // Timeout after 10 seconds if a thread has been waiting for a connection
      socketTimeoutMS: 30000, // Close sockets after 30 seconds of inactivity (reduced from 45)
      connectTimeoutMS: 20000 // Timeout connection attempts after 20 seconds (reduced from 30)
    });
    this.db = client.db(dbName);
    console.log('Connected to database');

    return this.db;
  }
  
  static getDb(): Db {
    return this.db;
  }
  
  // Method to track change streams
  static trackChangeStream(changeStream: any): void {
    this.activeChangeStreams.add(changeStream);
  }
  
  // Method to remove change stream from tracking
  static untrackChangeStream(changeStream: any): void {
    this.activeChangeStreams.delete(changeStream);
  }
  
  // Method to close all active change streams
  static closeAllChangeStreams(): void {
    console.log(`Closing ${this.activeChangeStreams.size} active change streams`);
    for (const stream of this.activeChangeStreams) {
      try {
        stream.close();
      } catch (error) {
        console.error('Error closing change stream:', error);
      }
    }
    this.activeChangeStreams.clear();
  }
}