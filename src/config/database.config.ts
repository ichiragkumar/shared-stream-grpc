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
      maxPoolSize: 50, // Hard limit on pool size to prevent unbounded growth
      minPoolSize: 5,  // Keep a few connections ready
      maxIdleTimeMS: 15000, // Close idle connections very quickly (15 seconds)
      connectTimeoutMS: 20000, // Reasonable connection timeout
      socketTimeoutMS: 30000, // Increased socket timeout to handle slow networks
      serverSelectionTimeoutMS: 20000, // Reasonable server selection timeout
      heartbeatFrequencyMS: 10000, // Regular server checks
      compressors: 'zlib', // Use compression to reduce network load
      // Use existing connections more efficiently
      maxConnecting: 10, // Limit concurrent connection attempts
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
