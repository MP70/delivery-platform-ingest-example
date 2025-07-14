#!/usr/bin/env node

import { Database } from './src/database.js';
import { Logger, ProcessingError, handleError } from './src/utils.js';

const db = new Database();

async function seed() {
  try {
    Logger.info('Seeding platforms...');
    
    // Create platforms and get their IDs
    const deliveryPlatform1Id = await db.upsertPlatform('DeliveryPlatform1');
    const deliveryPlatform3Id = await db.upsertPlatform('DeliveryPlatform3'); 
    const deliveryPlatform2Id = await db.upsertPlatform('DeliveryPlatform2');
    
    Logger.info('Seeding integrations...');
    
    // DeliveryPlatform3 integration with proper transformations
    const deliveryPlatform3Integration = {
      name: 'deliveryplatform3_total_order',
      platform_id: deliveryPlatform3Id,
      tables: ['orders', 'restaurants'],
      field_mapping: {
        'Partner': { target: 'restaurant_name', required: true },
        'Order Id': { target: 'platform_order_id', required: true },
        'Order Status': { target: 'order_status_raw', transform: 'deliveryPlatform3OrderStatus' },
        'Total Order Status - Customer Cancelled': { target: 'customer_cancelled_count', type: 'number' as const },
        'Total Order Status - Partner Cancelled': { target: 'partner_cancelled_count', type: 'number' as const },
        'Order Datetime': { target: 'order_datetime', type: 'date' as const, transform: 'parseDate' },
        'Total Total Order Value': { target: 'order_value', type: 'number' as const },
        'Total Applied Discount Amount': { target: 'discount_amount', type: 'number' as const },
        'Total Basket Size': { target: 'basket_size', type: 'number' as const },
        'Delivery/Collection': { target: 'delivery_type', type: 'enum' as const, enum_values: ['Delivery', 'Collection'], transform: 'deliveryType' },
        'Average Courier Arrival to Collected': { target: 'total_delivery_time_minutes', type: 'number' as const, transform: 'timeToMinutes' }
      },
      is_active: true
    };
    
    await db.upsertIntegration(deliveryPlatform3Integration as any);
    Logger.success(`${deliveryPlatform3Integration.name} (${deliveryPlatform3Integration.tables.join(', ')})`);
    
    // DeliveryPlatform1 Order History integration
    const deliveryPlatform1OrderIntegration = {
      name: 'deliveryplatform1_order_history',
      platform_id: deliveryPlatform1Id,
      tables: ['orders', 'restaurants'],
      field_mapping: {
        'Restaurant': { target: 'restaurant_name', required: true },
        'External restaurant ID': { target: 'restaurant_external_id' },
        'Order ID': { target: 'platform_order_id', required: true },
        'Order status': { target: 'order_status', type: 'enum' as const, enum_values: ['completed', 'canceled'], transform: 'deliveryPlatform1OrderStatus', required: true },
        'Completed?': { target: 'completed_flag', type: 'boolean' as const, transform: 'deliveryPlatform1Boolean' },
        'Cancelled by': { target: 'cancelled_by', transform: 'deliveryPlatform1CancelledBy' },
        'Ticket size': { target: 'order_value', type: 'number' as const },
        'Menu item count': { target: 'basket_size', type: 'number' as const },
        'Currency code': { target: 'currency_code' },
        'Time customer ordered': { target: 'order_datetime', type: 'date' as const },
        'Time to confirm': { target: 'prep_time_minutes', type: 'number' as const, transform: 'timeToMinutes' },
        'Courier waiting time (restaurant)': { target: 'courier_wait_time_minutes', type: 'number' as const, transform: 'timeToMinutes' },
        'Total delivery time': { target: 'total_delivery_time_minutes', type: 'number' as const, transform: 'timeToMinutes' },
        'Total prep & hand-off time': { target: 'restaurant_wait_time_minutes', type: 'number' as const, transform: 'timeToMinutes' },
        'Fulfilment Type': { target: 'delivery_type', type: 'enum' as const, enum_values: ['Delivery', 'Pickup'], transform: 'deliveryType' }
      },
      is_active: true
    };
    
    await db.upsertIntegration(deliveryPlatform1OrderIntegration as any);
    Logger.success(`${deliveryPlatform1OrderIntegration.name} (${deliveryPlatform1OrderIntegration.tables.join(', ')})`);
    
    // DeliveryPlatform1 Rating integration
    const deliveryPlatform1RatingIntegration = {
      name: 'deliveryplatform1_rating',
      platform_id: deliveryPlatform1Id,
      tables: ['ratings'],
      field_mapping: {
        'Restaurant': { target: 'restaurant_name', required: true },
        'External restaurant ID': { target: 'restaurant_external_id' },
        'Order ID': { target: 'platform_order_id', required: true },
        'Rating value': { target: 'rating_value', type: 'number' as const, required: true },
        'Rating date': { target: 'rating_date', type: 'date' as const },
        'Comment': { target: 'comment' }
      },
      is_active: true
    };
    
    await db.upsertIntegration(deliveryPlatform1RatingIntegration as any);
    Logger.success(`${deliveryPlatform1RatingIntegration.name} (${deliveryPlatform1RatingIntegration.tables.join(', ')})`);
    
    // DeliveryPlatform2 Business Segments integration
    const deliveryPlatform2Integration = {
      name: 'deliveryplatform2_business_segments',
      platform_id: deliveryPlatform2Id,
      tables: ['orders', 'restaurants'],
      field_mapping: {
        'Partner Restaurant Name': { target: 'restaurant_name', required: true },
        'Order Order ID': { target: 'platform_order_id', required: true },
        'Common Business Segments  Order Date': { target: 'order_datetime', type: 'date' as const },
        'Common Business Segments Order Minute5 of Day': { target: 'order_time', transform: 'parseDeliveryPlatform2Time' },
        'Order Order Value': { target: 'order_value', type: 'number' as const },
        'Order Auto Accept Status': { target: 'auto_accept_status', transform: 'deliveryPlatform2AcceptStatus' },
        'Logistics Restaurant Wait Time (All Riders, mins)': { target: 'restaurant_wait_time_minutes', type: 'number' as const, transform: 'timeToMinutes' },
        'Order Rating': { target: 'rating_value', type: 'number' as const }
      },
      is_active: true
    };
    
    await db.upsertIntegration(deliveryPlatform2Integration as any);
    Logger.success(`${deliveryPlatform2Integration.name} (${deliveryPlatform2Integration.tables.join(', ')})`);
    
    Logger.success('Seeding completed successfully!');
    process.exit(0);
  } catch (error: unknown) {
    handleError(error, 'seeding');
  }
}

seed(); 