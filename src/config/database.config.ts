import { MongoClient, Db, MongoClientOptions } from 'mongodb';
import { config } from 'dotenv';

config();

export class DatabaseService {
  private static db: Db;
  private static client: MongoClient;
  
  static async connect(): Promise<Db> {
    if (this.db && this.client) return this.db;

    const mongoUri = process.env.MONGODB_URI;
    const dbName = process.env.DB_NAME;

    if (!mongoUri || !dbName) {
      throw new Error('Missing MongoDB URI or Database Name in .env');
    }

    // Configure MongoDB connection pool settings - critical for fixing connection leaks
    const options: MongoClientOptions = {
      maxPoolSize: 20, // Reduced pool size to prevent exhaustion
      minPoolSize: 3,   // Minimal connections needed
      maxIdleTimeMS: 30000, // Close idle connections much faster (30 seconds)
      connectTimeoutMS: 10000, // Shorter connection timeout
      socketTimeoutMS: 20000, // Shorter socket timeout
      // waitQueueTimeoutMS: 5000, // Shorter wait queue timeout
      serverSelectionTimeoutMS: 10000, // Faster server selection timeout
      heartbeatFrequencyMS: 10000, // More frequent server checks
      compressors: 'zlib', // Use compression to reduce network load
    };
    
    try {
      // Create a single client that will be reused throughout the application
      this.client = await MongoClient.connect(mongoUri, options);
      this.db = this.client.db(dbName);
      
      // Monitor connection pool events for debugging
      this.client.on('connectionPoolCreated', () => {
        console.log('MongoDB connection pool created');
      });
      
      this.client.on('connectionPoolClosed', () => {
        console.log('MongoDB connection pool closed');
      });

      // Handle process termination for clean shutdown
      process.on('SIGINT', async () => {
        await this.disconnect();
        process.exit(0);
      });
      
      process.on('SIGTERM', async () => {
        await this.disconnect();
        process.exit(0);
      });
      
      console.log('Connected to database with optimized connection pool');
      return this.db;
    } catch (error) {
      console.error('Failed to connect to MongoDB:', error);
      throw error;
    }
  }
  
  static getDb(): Db {
    if (!this.db) {
      throw new Error('Database connection not established. Call connect() first.');
    }
    return this.db;
  }
  
  static getClient(): MongoClient {
    if (!this.client) {
      throw new Error('MongoDB client not established. Call connect() first.');
    }
    return this.client;
  }
  
  static async disconnect(): Promise<void> {
    if (this.client) {
      console.log('Closing MongoDB connection...');
      await this.client.close();
      // Use type assertions to handle the null assignment
      this.client = null as unknown as MongoClient;
      this.db = null as unknown as Db;
      console.log('MongoDB connection closed.');
    }
  }
}
