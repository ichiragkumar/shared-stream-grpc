import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MongoClient, Db, Collection, Document, ChangeStream } from 'mongodb';
import { Observable, Subject, filter } from 'rxjs';
import { LoggerService } from '../logger/logger.service';
import { DatabaseService } from './database.config';

/**
 * A service that maintains a single MongoDB change stream for the entire application
 * and allows multiple subscribers to filter the events they're interested in.
 * 
 * This approach drastically reduces the number of MongoDB connections by sharing
 * a single change stream connection among all subscribers.
 */
@Injectable()
export class SharedChangeStreamService implements OnModuleInit, OnModuleDestroy {
  private db: Db;
  private client: MongoClient;
  private changeStreams: Map<string, { stream: ChangeStream; subject: Subject<any>; refCount: number }> = new Map();
  
  constructor(private readonly logger: LoggerService) {}
  
  async onModuleInit() {
    try {
      this.db = DatabaseService.getDb();
      this.client = DatabaseService.getClient();
      this.logger.log('SharedChangeStreamService initialized');
      
      // Start monitoring connection pool periodically
      this.startMonitoring();
    } catch (error) {
      this.logger.error(`Failed to initialize SharedChangeStreamService: ${error.message}`);
      throw error;
    }
  }
  
  /**
   * Creates or reuses a shared change stream for a collection
   * @param collectionName The name of the collection to watch
   * @param filterFn A function to filter change events
   * @returns An observable of filtered change events
   */
  getSharedChangeStream<T extends Document>(
    collectionName: string,
    filterFn: (change: any) => boolean = () => true
  ): Observable<T> {
    // Create a new subject for this subscriber
    const subject = new Subject<T>();
    
    // Check if we already have a change stream for this collection
    if (!this.changeStreams.has(collectionName)) {
      this.logger.log(`Creating new shared change stream for collection: ${collectionName}`);
      
      // Create a new change stream for this collection
      const collection = this.db.collection(collectionName);
      const stream = collection.watch([], { fullDocument: 'updateLookup' });
      
      // Create a new subject to broadcast all events from this collection
      const collectionSubject = new Subject<any>();
      
      // Forward all events from the change stream to the subject
      stream.on('change', (change) => {
        collectionSubject.next(change);
      });
      
      // Handle errors
      stream.on('error', (error) => {
        this.logger.error(`Change stream error for collection ${collectionName}: ${error.message}`);
        collectionSubject.error(error);
        
        // Clean up and recreate the stream after a short delay
        setTimeout(() => {
          this.recreateStream(collectionName);
        }, 5000);
      });
      
      // Store the stream and subject
      this.changeStreams.set(collectionName, {
        stream,
        subject: collectionSubject,
        refCount: 1
      });
    } else {
      // Increment the reference count for this collection's change stream
      const entry = this.changeStreams.get(collectionName);
      if (entry) {
        entry.refCount++;
        this.changeStreams.set(collectionName, entry);
        this.logger.log(`Reusing shared change stream for collection: ${collectionName}, refCount: ${entry.refCount}`);
      } else {
        this.logger.error(`Entry for ${collectionName} was undefined when trying to increment refCount.`);
      }
    }
    
    // Get the collection subject
    const entry = this.changeStreams.get(collectionName);
    const collectionSubject = entry?.subject;
    
    // Subscribe to the collection subject and filter events for this subscriber
    let subscription;
    if (collectionSubject) {
      subscription = collectionSubject
        .pipe(filter(filterFn))
        .subscribe({
          next: (change) => subject.next(change),
          error: (error) => subject.error(error),
          complete: () => subject.complete()
        });
    } else {
      // If collectionSubject is undefined, just return an empty observable
      this.logger.error(`Collection subject for ${collectionName} was undefined`);
      // Complete the subject immediately
      setTimeout(() => subject.complete(), 0);
    }
    
    // When this subscriber unsubscribes, clean up
    subject.subscribe({
      complete: () => {
        if (subscription) {
          subscription.unsubscribe();
        }
        this.releaseChangeStream(collectionName);
      }
    });
    
    return subject.asObservable();
  }
  
  /**
   * Decrements the reference count for a collection's change stream
   * and closes it if no more subscribers
   */
  private releaseChangeStream(collectionName: string) {
    if (this.changeStreams.has(collectionName)) {
      const entry = this.changeStreams.get(collectionName);
      if (entry) {
        entry.refCount--;
        
        this.logger.log(`Released change stream for collection: ${collectionName}, refCount: ${entry.refCount}`);
        
        if (entry.refCount <= 0) {
          this.logger.log(`Closing change stream for collection: ${collectionName}`);
          entry.stream.close();
          entry.subject.complete();
          this.changeStreams.delete(collectionName);
        } else {
          this.changeStreams.set(collectionName, entry);
        }
      } else {
        this.logger.error(`Entry for ${collectionName} was undefined when trying to release refCount.`);
      }
    }
  }
  
  /**
   * Recreates a change stream for a collection after an error
   */
  private recreateStream(collectionName: string) {
    if (this.changeStreams.has(collectionName)) {
      const entry = this.changeStreams.get(collectionName);
      
      try {
        // Close the old stream if it's still open
        if (entry) {
          entry.stream.close();
        }
      } catch (error) {
        this.logger.error(`Error closing change stream for ${collectionName}: ${error.message}`);
      }
      
      // Create a new stream
      const collection = this.db.collection(collectionName);
      const stream = collection.watch([], { fullDocument: 'updateLookup' });
      
      // Forward all events from the new stream to the existing subject
      stream.on('change', (change) => {
        if (entry) {
          entry.subject.next(change);
        }
      });
      
      // Handle errors
      stream.on('error', (error) => {
        this.logger.error(`Change stream error for collection ${collectionName}: ${error.message}`);
        
        // Clean up and recreate the stream after a short delay
        setTimeout(() => {
          this.recreateStream(collectionName);
        }, 5000);
      });
      
      // Update the stream in the map
      if (entry) {
        this.changeStreams.set(collectionName, {
          stream,
          subject: entry.subject,
          refCount: entry.refCount
        });
      }
      
      this.logger.log(`Recreated change stream for collection: ${collectionName}`);
    }
  }
  
  /**
   * Monitors the MongoDB connection pool
   */
  private startMonitoring() {
    setInterval(async () => {
      try {
        const adminDb = this.client.db('admin');
        const stats = await adminDb.command({ serverStatus: 1 });
        
        this.logger.log('Connection pool monitoring', {
          activeConnections: stats.connections.current,
          availableConnections: stats.connections.available,
          totalCreated: stats.connections.totalCreated,
          activeChangeStreams: this.changeStreams.size
        });
      } catch (error) {
        this.logger.error(`Error monitoring connections: ${error.message}`);
      }
    }, 30000); // Check every 30 seconds
  }
  
  /**
   * Closes all change streams when the application shuts down
   */
  async onModuleDestroy() {
    for (const [collectionName, { stream, subject }] of this.changeStreams.entries()) {
      try {
        stream.close();
        subject.complete();
        this.logger.log(`Closed change stream for collection: ${collectionName}`);
      } catch (error) {
        this.logger.error(`Error closing change stream for ${collectionName}: ${error.message}`);
      }
    }
    
    this.changeStreams.clear();
    this.logger.log('SharedChangeStreamService destroyed, all change streams closed');
  }
}
