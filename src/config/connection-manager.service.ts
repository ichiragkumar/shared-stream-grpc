import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MongoClient, Db, ChangeStream, Collection, Document, ChangeStreamDocument } from 'mongodb';
import { LoggerService } from '../logger/logger.service';
import { DatabaseService } from './database.config';

/**
 * Service to manage MongoDB change streams and prevent connection leaks
 * This service tracks all active change streams and ensures they're properly closed
 */
@Injectable()
export class ConnectionManagerService implements OnModuleInit, OnModuleDestroy {
  private activeStreams: Map<string, ChangeStream> = new Map();
  private streamCount = 0;
  private db: Db;
  private client: MongoClient;
  
  constructor(private readonly logger: LoggerService) {}
  
  async onModuleInit() {
    try {
      this.db = DatabaseService.getDb();
      this.client = DatabaseService.getClient();
      this.logger.log('ConnectionManagerService initialized');
      
      // Start periodic monitoring of connection pool and active streams
      this.startMonitoring();
    } catch (error) {
      this.logger.error(`Failed to initialize ConnectionManagerService: ${error.message}`);
      throw error;
    }
  }
  
  /**
   * Creates a change stream with automatic cleanup and tracking
   */
  createChangeStream<T extends Document>(collection: Collection, pipeline: any[] = [], options: any = {}): ChangeStream<T> {
    const streamId = `stream_${++this.streamCount}_${Date.now()}`;
    const changeStream: ChangeStream<T> = collection.watch(pipeline, options);
    
    // Add cleanup handler
    changeStream.on('close', () => {
      this.activeStreams.delete(streamId);
      this.logger.log(`Change stream ${streamId} closed, ${this.activeStreams.size} active streams remaining`);
    });
    
    changeStream.on('error', (error) => {
      this.logger.error(`Change stream ${streamId} error: ${error.message}`);
      // Ensure the stream is removed from tracking when there's an error
      this.activeStreams.delete(streamId);
    });
    
    // Track the stream
    this.activeStreams.set(streamId, changeStream);
    this.logger.log(`New change stream ${streamId} created, ${this.activeStreams.size} active streams`);
    
    return changeStream;
  }
  
  /**
   * Closes a specific change stream
   */
  closeStream(changeStream: ChangeStream) {
    if (changeStream) {
      try {
        changeStream.close();
      } catch (error) {
        this.logger.error(`Error closing change stream: ${error.message}`);
      }
    }
  }
  
  /**
   * Closes all active change streams
   */
  async closeAllStreams() {
    this.logger.log(`Closing all ${this.activeStreams.size} active change streams`);
    
    for (const [streamId, stream] of this.activeStreams.entries()) {
      try {
        await stream.close();
        this.logger.log(`Closed stream ${streamId}`);
      } catch (error) {
        this.logger.error(`Error closing stream ${streamId}: ${error.message}`);
      }
    }
    
    this.activeStreams.clear();
    this.logger.log('All change streams closed');
  }
  
  /**
   * Start monitoring connection pool and active streams
   */
  private startMonitoring() {
    // Check every minute
    setInterval(async () => {
      try {
        const adminDb = this.client.db('admin');
        const stats = await adminDb.command({ serverStatus: 1 });
        
        this.logger.log('Connection pool monitoring', {
          activeConnections: stats.connections.current,
          availableConnections: stats.connections.available,
          totalCreated: stats.connections.totalCreated,
          activeStreams: this.activeStreams.size
        });
        
        // If we're approaching the connection limit, force close idle streams
        if (stats.connections.current > 450) {
          this.logger.warn('Connection pool near capacity, forcing cleanup');
          await this.closeAllStreams();
        }
      } catch (error) {
        this.logger.error(`Error monitoring connections: ${error.message}`);
      }
    }, 60000); // 1 minute
  }
  
  async onModuleDestroy() {
    await this.closeAllStreams();
    this.logger.log('ConnectionManagerService destroyed');
  }
}
