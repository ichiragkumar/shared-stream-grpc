import { Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { Db, Filter, Document } from 'mongodb';
import { DatabaseService } from 'src/config/database.config';

interface BusinessDocument extends Document {
  _id: string;
  description: Record<string, string>;
  title: Record<string, string>;
  logo: Record<string, Record<string, Record<string, string>>>;
  categories?: string[];
  suspended: boolean;
  sponsorshipType?: string | null;
  createdAt: Date;
}

@Injectable()
export class BusinessService {
  private db: Db;

  constructor() {}

  async onModuleInit() {
    this.db = await DatabaseService.connect();
  }

  async getBusinessById(id: string, languageCode: string = 'en', brightness: string = 'light') {
    if (!id) {
      throw new BadRequestException('Invalid request: id is missing.');
    }

    try {
      const filter: Filter<BusinessDocument> = { _id: id } as Filter<BusinessDocument>;
      const business = await this.db
        .collection<BusinessDocument>('businesses')
        .findOne(filter);

      if (!business) {
        throw new NotFoundException(`Business with id ${id} not found.`);
      }

      const localizedName = business.description?.[languageCode] || business.description?.['en'] || 'Unknown';
      const localizedTitle = business.title?.[languageCode] || business.title?.['en'] || 'Unknown';
      const image = business.logo?.[brightness]?.[languageCode] || business.logo?.[brightness]?.['en'] || null;

      return {
        id: business._id,
        name: localizedName,
        title: localizedTitle,
        image,
        categories: business.categories || [],
        createdAt: business.createdAt,
        suspended: business.suspended,
        sponsorshipType: business.sponsorshipType || null,
      };
    } catch (error) {
      throw new Error('Internal Server Error :getBusinessesWithId');

    }
  }

  async getBusinessesWithIds(ids: string[], languageCode: string = 'en', brightness: string = 'light') {
    if (!ids || ids.length === 0) {
      throw new BadRequestException("'ids' must be a non-empty array. : getBusinessesWithIds");
    }

    try {
      const filter: Filter<BusinessDocument> = { 
        _id: { $in: ids } 
      } as Filter<BusinessDocument>;
      
      const businesses = await this.db
        .collection<BusinessDocument>('businesses')
        .find(filter)
        .toArray();

      if (!businesses.length) {
        throw new NotFoundException('No businesses found for the provided IDs.: getBusinessesWithIds');
      }

      return businesses.map(business => ({
        id: business._id,
        name: business.description?.[languageCode] || business.description?.['en'] || 'Unknown',
        title: business.title?.[languageCode] || business.title?.['en'] || 'Unknown',
        image: business.logo?.[brightness]?.[languageCode] || business.logo?.[brightness]?.['en'] || null,
        categories: business.categories || [],
        createdAt: business.createdAt,
        suspended: business.suspended,
        sponsorshipType: business.sponsorshipType || null,
      }));
    } catch (error) {
      console.log("i am her",error)
      throw new Error('Internal Server Error :getBusinessesWithIds');
    }
  }
}