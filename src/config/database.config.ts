import { MongoClient, Db } from 'mongodb';
import { config } from 'dotenv';

config();

export class DatabaseService {
  private static db: Db;
  
  static async connect(): Promise<Db> {
    if (this.db) return this.db;

    const mongoUri = process.env.MONGODB_URI;
    const dbName = process.env.DB_NAME;

    if (!mongoUri || !dbName) {
      throw new Error('Missing MongoDB URI or Database Name in .env');
    }

    console.log("i am trying to connect my  baby")
    
    const client = await MongoClient.connect(mongoUri);
    this.db = client.db(dbName);
    console.log('Connected to database');

    return this.db;
  }
  
  static getDb(): Db {
    return this.db;
  }
}