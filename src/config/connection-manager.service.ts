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
    // Check more frequently - every 15 seconds
    setInterval(async () => {
      try {
        const adminDb = this.client.db('admin');
        const stats = await adminDb.command({ serverStatus: 1 });
        
        // Log every minute instead of every check to reduce noise
        const shouldLog = new Date().getSeconds() < 15;
        if (shouldLog || stats.connections.current > 100) {
          this.logger.log('Connection pool monitoring', {
            activeConnections: stats.connections.current,
            availableConnections: stats.connections.available,
            totalCreated: stats.connections.totalCreated,
            maxPoolSize: 50, // Should match the setting in database.config.ts
            activeStreams: this.activeStreams.size
          });
        }
        
        // Three-tier approach to connection management
        if (stats.connections.current > 45) { // 90% of max pool size
          this.logger.warn('Connection pool critical, forcing cleanup of all streams');
          await this.closeAllStreams();
        } else if (stats.connections.current > 40) { // 80% of max pool size
          this.logger.warn('Connection pool near capacity, cleaning up idle streams');
          await this.cleanupIdleStreams();
        } else if (stats.connections.current > 30) { // 60% of max pool size
          this.logger.log('Connection pool moderately busy, monitoring closely');
        }
      } catch (error) {
        this.logger.error(`Error monitoring connections: ${error.message}`);
      }
    }, 15000); // Check every 15 seconds
  }
  
  /**
   * Intelligently close idle streams when approaching pool capacity
   * This attempts to identify and close less active streams first
   */
  private async cleanupIdleStreams() {
    // If we have only a few streams, don't bother
    if (this.activeStreams.size < 5) {
      return;
    }
    
    // Close approximately 30% of streams
    const streamsToClose = Math.ceil(this.activeStreams.size * 0.3);
    this.logger.log(`Attempting to close ${streamsToClose} idle streams out of ${this.activeStreams.size} total`);
    
    // Convert to array for sorting/slicing
    const streamEntries = Array.from(this.activeStreams.entries());
    
    // Start by closing the oldest streams
    const streamsToCloseIds = streamEntries
      .filter(([id]) => {
        // Simple heuristic: extract timestamp from ID
        const timestampStr = id.split('_')[2];
        if (!timestampStr) return false;
        
        const timestamp = parseInt(timestampStr, 10);
        const ageInMinutes = (Date.now() - timestamp) / (1000 * 60);
        
        // Close streams older than 15 minutes first
        return ageInMinutes > 15;
      })
      .slice(0, streamsToClose)
      .map(([id]) => id);
    
    // Close the selected streams
    for (const streamId of streamsToCloseIds) {
      const stream = this.activeStreams.get(streamId);
      if (stream) {
        try {
          await stream.close();
          this.activeStreams.delete(streamId);
          this.logger.log(`Closed idle stream ${streamId}`);
        } catch (error) {
          this.logger.error(`Error closing idle stream ${streamId}: ${error.message}`);
        }
      }
    }
    
    this.logger.log(`Closed ${streamsToCloseIds.length} idle streams, ${this.activeStreams.size} remaining`);
  }
  
  async onModuleDestroy() {
    await this.closeAllStreams();
    this.logger.log('ConnectionManagerService destroyed');
  }
}
