import { MongoClient, Db } from 'mongodb';

export class DatabaseService {
  private static db: Db;
  
  static async connect(): Promise<Db> {
    if (this.db) return this.db;
    
    const client = await MongoClient.connect('mongodb+srv://ahande:7CrAMxYK9XcrSIFn@cluster0.hb1oi.mongodb.net/');
    this.db = client.db('firestore-waw');
    console.log('Connected to database');

    return this.db;
  }
  
  static getDb(): Db {
    return this.db;
  }
}