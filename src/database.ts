/**
 *  Database Layer 
 */

import pkg from 'pg';
const { Pool } = pkg;
import { Integration, FieldMap, OrderData, JobUpdate } from './types.js';
import { DatabaseError, Validators, Logger } from './utils.js';

const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'orders_db',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
  statement_timeout: 60000,
  query_timeout: 60000,
});

export class Database {
  
  async upsertRestaurant(name: string, platformId: number, externalId?: string): Promise<number> {
    try {
      // Input validation
      if (!Validators.isValidString(name)) {
        throw new DatabaseError('Restaurant name is required and must be a non-empty string', { name });
      }
      
      if (!Validators.isValidNumber(platformId)) {
        throw new DatabaseError('Platform ID must be a valid number', { platformId });
      }

      const nameStr = String(name);
      const platformIdNum = Number(platformId);
      
      // First try to find existing restaurant by external_id if provided
      if (externalId && externalId.trim() !== '') {
        const existingByExternalId = await pool.query(
          'SELECT id FROM restaurants WHERE platform_id = $1 AND external_id = $2',
          [platformIdNum, String(externalId)]
        );
        
        if (existingByExternalId.rows.length > 0) {
          // Update the name if different and return existing ID
          await pool.query(
            'UPDATE restaurants SET name = $1 WHERE id = $2',
            [nameStr, existingByExternalId.rows[0].id]
          );
          return existingByExternalId.rows[0].id;
        }
      }
      
      // Then try to find by name and platform
      const existingByName = await pool.query(
        'SELECT id FROM restaurants WHERE platform_id = $1 AND name = $2',
        [platformIdNum, nameStr]
      );
      
      if (existingByName.rows.length > 0) {
        // Update external_id if provided and return existing ID
        if (externalId && externalId.trim() !== '') {
          await pool.query(
            'UPDATE restaurants SET external_id = $1 WHERE id = $2',
            [String(externalId), existingByName.rows[0].id]
          );
        }
        return existingByName.rows[0].id;
      }
      
      // Create new restaurant
      const result = await pool.query(
        'INSERT INTO restaurants (name, platform_id, external_id) VALUES ($1, $2, $3) RETURNING id',
        [nameStr, platformIdNum, externalId && externalId.trim() !== '' ? String(externalId) : null]
      );
      return result.rows[0].id;
    } catch (error: unknown) {
      if (error instanceof DatabaseError) {
        throw error;
      }
      throw new DatabaseError('Failed to upsert restaurant', { 
        originalError: error, 
        name, 
        platformId, 
        externalId 
      });
    }
  }

  async upsertPlatform(name: string): Promise<number> {
    try {
      // Input validation
      if (!Validators.isValidString(name)) {
        throw new DatabaseError('Platform name is required and must be a non-empty string', { name });
      }

      const result = await pool.query(
        'INSERT INTO platforms (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id',
        [name]
      );
      return result.rows[0].id;
    } catch (error: unknown) {
      if (error instanceof DatabaseError) {
        throw error;
      }
      throw new DatabaseError('Failed to upsert platform', { 
        originalError: error, 
        name 
      });
    }
  }

  async upsertIntegration(integration: Partial<Integration>): Promise<number> {
    const result = await pool.query(
      `INSERT INTO integrations (name, platform_id, field_mapping, tables, is_active) 
       VALUES ($1, $2, $3::jsonb, $4, $5) 
       ON CONFLICT (name) DO UPDATE SET 
         platform_id = EXCLUDED.platform_id,
         field_mapping = EXCLUDED.field_mapping,
         tables = EXCLUDED.tables,
         is_active = EXCLUDED.is_active
       RETURNING id`,
      [
        integration.name,
        integration.platform_id,
        JSON.stringify(integration.field_mapping),
        integration.tables,
        integration.is_active ?? true
      ]
    );
    return result.rows[0].id;
  }

  async upsertOrder(orderData: OrderData): Promise<number> {
    try {
      // Input validation
      if (!Validators.isValidNumber(orderData.platform_id)) {
        throw new DatabaseError('Platform ID must be a valid number', { platformId: orderData.platform_id });
      }
      
      if (!Validators.isValidString(orderData.platform_order_id)) {
        throw new DatabaseError('Platform order ID is required and must be a non-empty string', { 
          platformOrderId: orderData.platform_order_id 
        });
      }
      
      if (!Validators.isValidNumber(orderData.restaurant_id)) {
        throw new DatabaseError('Restaurant ID must be a valid number', { restaurantId: orderData.restaurant_id });
      }

      const result = await pool.query(
        `INSERT INTO orders (
          platform_id, platform_order_id, restaurant_id, order_status, 
          delivery_type, order_value, basket_size, discount_amount, 
          order_datetime, restaurant_wait_time_minutes, total_delivery_time_minutes, 
          courier_wait_time_minutes, prep_time_minutes, currency_code, auto_accept_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (platform_id, platform_order_id) 
        DO UPDATE SET 
          restaurant_id = EXCLUDED.restaurant_id,
          order_status = EXCLUDED.order_status,
          delivery_type = EXCLUDED.delivery_type,
          order_value = EXCLUDED.order_value,
          basket_size = EXCLUDED.basket_size,
          discount_amount = EXCLUDED.discount_amount,
          order_datetime = EXCLUDED.order_datetime,
          restaurant_wait_time_minutes = EXCLUDED.restaurant_wait_time_minutes,
          total_delivery_time_minutes = EXCLUDED.total_delivery_time_minutes,
          courier_wait_time_minutes = EXCLUDED.courier_wait_time_minutes,
          prep_time_minutes = EXCLUDED.prep_time_minutes,
          currency_code = EXCLUDED.currency_code,
          auto_accept_status = EXCLUDED.auto_accept_status
        RETURNING id`,
        [
          orderData.platform_id,
          orderData.platform_order_id,
          orderData.restaurant_id,
          orderData.order_status,
          orderData.delivery_type || 'UNKNOWN',
          orderData.order_value,
          orderData.basket_size,
          orderData.discount_amount,
          orderData.order_datetime,
          orderData.restaurant_wait_time_minutes,
          orderData.total_delivery_time_minutes,
          orderData.courier_wait_time_minutes,
          orderData.prep_time_minutes,
          orderData.currency_code || 'GBP',
          orderData.auto_accept_status
        ]
      );
      return result.rows[0].id;
    } catch (error: unknown) {
      if (error instanceof DatabaseError) {
        throw error;
      }
      throw new DatabaseError('Failed to upsert order', { 
        originalError: error, 
        orderData 
      });
    }
  }

  async upsertRating(ratingData: any, restaurantId: number, platformId: number): Promise<void> {
    // Check if rating already exists for this order
    if (ratingData.platform_order_id) {
      const existing = await pool.query(
        'SELECT id FROM ratings WHERE platform_id = $1 AND platform_order_id = $2 AND rating_type = $3',
        [platformId, ratingData.platform_order_id, ratingData.rating_type]
      );
      
      if (existing.rows.length > 0) {
        // Update existing rating
        await pool.query(
          'UPDATE ratings SET rating_value = $1, comment = $2 WHERE id = $3',
          [ratingData.rating_value, ratingData.comment, existing.rows[0].id]
        );
        return;
      }
    }
    
    // Insert new rating
    await pool.query(
      'INSERT INTO ratings (platform_order_id, restaurant_id, platform_id, rating_value, rating_type, comment, rating_date) VALUES ($1, $2, $3, $4, $5, $6, $7)',
      [
        ratingData.platform_order_id || null,
        restaurantId,
        platformId,
        ratingData.rating_value,
        ratingData.rating_type,
        ratingData.comment,
        ratingData.rating_date || null
      ]
    );
  }

  private integrationCache = new Map<string, Integration>();
  
  async getIntegrationByHeaders(headers: string[]): Promise<Integration | null> {
    const cacheKey = [...headers].sort().join('|');
    if (this.integrationCache.has(cacheKey)) {
      return this.integrationCache.get(cacheKey)!;
    }

    const result = await pool.query('SELECT * FROM integrations WHERE is_active = true');
    
    let bestMatch: Integration | null = null;
    let bestScore = 0;
    
    for (const integration of result.rows) {
      const fieldMappingKeys = Object.keys(integration.field_mapping);
      const matchCount = fieldMappingKeys.filter(key => headers.includes(key)).length;
      const score = matchCount / fieldMappingKeys.length;
      
      if (score > 0.7 && score > bestScore) {
        bestMatch = integration;
        bestScore = score;
      }
    }

    if (bestMatch) {
      this.integrationCache.set(cacheKey, bestMatch);
    }
    
    return bestMatch;
  }

  async isFileProcessed(integrationId: number, filePath: string, fileHash: string): Promise<boolean> {
    const result = await pool.query(
      'SELECT 1 FROM data_source_files WHERE integration_id = $1 AND file_hash = $2 LIMIT 1',
      [integrationId, fileHash]
    );
    return result.rows.length > 0;
  }

  async createJob(integrationId: number, filePath: string, totalRows: number): Promise<number> {
    const result = await pool.query(
      'INSERT INTO ingestion_jobs (integration_id, file_path, total_rows) VALUES ($1, $2, $3) RETURNING id',
      [integrationId, filePath, totalRows]
    );
    return result.rows[0].id;
  }

  async updateJob(jobId: number, stats: {
    status: string;
    totalRows?: number;
    processedRows?: number;
    insertedRows?: number;
    errorRows?: number;
    errorMessage?: string;
  }): Promise<void> {
    const query = `
      UPDATE ingestion_jobs 
      SET status = $2::TEXT, 
          completed_at = CASE WHEN $2 IN ('completed', 'failed') THEN NOW() ELSE completed_at END,
          total_rows = COALESCE($3::INTEGER, total_rows),
          processed_rows = COALESCE($4::INTEGER, processed_rows), 
          inserted_rows = COALESCE($5::INTEGER, inserted_rows),
          error_rows = COALESCE($6::INTEGER, error_rows),
          error_message = COALESCE($7::TEXT, error_message)
      WHERE id = $1::INTEGER`;
    
    await pool.query(query, [
      jobId, stats.status, 
      stats.totalRows || null, stats.processedRows || null, 
      stats.insertedRows || null, stats.errorRows || null, 
      stats.errorMessage || null
    ]);
  }

  async recordProcessedFile(integrationId: number, filePath: string, fileHash: string, totalRows: number, jobId: number): Promise<void> {
    await pool.query(
      'INSERT INTO data_source_files (integration_id, file_path, file_hash, total_rows, job_id) VALUES ($1, $2, $3, $4, $5)',
      [integrationId, filePath, fileHash, totalRows, jobId]
    );
  }

  async getJobs(limit: number = 20): Promise<JobUpdate[]> {
    const result = await pool.query(
      'SELECT * FROM ingestion_jobs ORDER BY created_at DESC LIMIT $1',
      [limit]
    );
    return result.rows;
  }

  async getIntegrationByName(name: string): Promise<Integration | null> {
    const result = await pool.query('SELECT * FROM integrations WHERE name = $1 AND is_active = true', [name]);
    return result.rows[0] || null;
  }

  async getIntegrations(): Promise<Integration[]> {
    const result = await pool.query('SELECT i.*, p.name as platform_name FROM integrations i JOIN platforms p ON i.platform_id = p.id WHERE i.is_active = true ORDER BY p.name, i.name');
    return result.rows;
  }

  async query(text: string, params?: any[]): Promise<any> {
    return pool.query(text, params);
  }

  async close(): Promise<void> {
    await pool.end();
  }

  async clearDatabase(): Promise<void> {
    try {
      Logger.info('Clearing database...');
      
      // Clear tables in order to respect foreign key constraints
      await pool.query('TRUNCATE TABLE data_source_files CASCADE');
      await pool.query('TRUNCATE TABLE ingestion_jobs CASCADE');
      await pool.query('TRUNCATE TABLE ratings CASCADE');
      await pool.query('TRUNCATE TABLE orders CASCADE');
      await pool.query('TRUNCATE TABLE restaurants CASCADE');
      await pool.query('TRUNCATE TABLE integrations CASCADE');
      await pool.query('TRUNCATE TABLE platforms CASCADE');
      
      Logger.success('Database cleared successfully');
    } catch (error: unknown) {
      throw new DatabaseError('Failed to clear database', { originalError: error });
    }
  }
} 