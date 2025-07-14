#!/usr/bin/env node

import { IngestionEngine } from './src/ingestion-engine.js';
import { Database } from './src/database.js';
import { Validators, Logger, ValidationError, ProcessingError, handleError } from './src/utils.js';


const db = new Database();
const ingestionEngine = new IngestionEngine(db);

async function processFile(filePath: string, integrationKey?: string): Promise<void> {
  try {
    // Input validation
    if (!Validators.isValidFilePath(filePath)) {
      throw new ValidationError('Invalid file path', { filePath });
    }

    if (integrationKey && !Validators.isValidIntegrationKey(integrationKey)) {
      throw new ValidationError('Invalid integration key', { integrationKey });
    }

    await ingestionEngine.processFile(filePath, integrationKey);
    Logger.success('File processed successfully');
  } catch (error: unknown) {
    handleError(error, 'file processing');
  }
}

async function analyseOrders(): Promise<void> {
  console.log('\n📊 ORDER ANALYSIS REPORT\n');
  
  try {
    // Basic metrics
    console.log('=== BASIC METRICS ===');
    
    // Orders per day
    const ordersPerDay = await db.query(`
      SELECT 
        DATE(order_datetime) as order_date,
        COUNT(*) as order_count,
        AVG(order_value) as avg_value,
        SUM(order_value) as total_value
      FROM orders 
      WHERE order_datetime IS NOT NULL
      GROUP BY DATE(order_datetime)
      ORDER BY order_date DESC
      LIMIT 10
    `);
    
    console.log('\n📅 Orders per day (last 10 days):');
    ordersPerDay.rows.forEach((row: any) => {
      console.log(`  ${row.order_date}: ${row.order_count} orders, £${Number(row.avg_value || 0).toFixed(2)} avg, £${Number(row.total_value || 0).toFixed(2)} total`);
    });

    // Average order value
    const avgOrderValue = await db.query(`
      SELECT 
        AVG(order_value) as avg_order_value,
        COUNT(*) as total_orders,
        SUM(order_value) as total_revenue
      FROM orders 
      WHERE order_value IS NOT NULL
    `);
    
    console.log(`\n💰 Overall average order value: £${Number(avgOrderValue.rows[0]?.avg_order_value || 0).toFixed(2)}`);
    console.log(`📦 Total orders: ${avgOrderValue.rows[0]?.total_orders || 0}`);
    console.log(`💵 Total revenue: £${Number(avgOrderValue.rows[0]?.total_revenue || 0).toFixed(2)}`);

    // Restaurant performance
    console.log('\n=== RESTAURANT PERFORMANCE ===');
    
    const restaurantRevenue = await db.query(`
      SELECT 
        r.name as restaurant_name,
        p.name as platform,
        COUNT(o.id) as order_count,
        COALESCE(SUM(o.order_value), 0) as total_revenue,
        COALESCE(AVG(o.order_value), 0) as avg_order_value,
        ROUND(
          (COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT', 'CANCELLED_CUSTOMER', 'CANCELLED_RESTAURANT') THEN 1 END)::decimal / COUNT(o.id)) * 100, 2
        ) as failure_rate_percent
      FROM restaurants r
      JOIN platforms p ON r.platform_id = p.id
      LEFT JOIN orders o ON r.id = o.restaurant_id
      WHERE r.id IN (SELECT restaurant_id FROM orders)
      GROUP BY r.id, r.name, p.name
      ORDER BY total_revenue DESC
      LIMIT 10
    `);

    console.log('\n🏆 Top 10 restaurants by revenue:');
    restaurantRevenue.rows.forEach((row: any, i: number) => {
      console.log(`  ${i+1}. ${row.restaurant_name} (${row.platform})`);
      console.log(`     Revenue: £${Number(row.total_revenue).toFixed(2)} | Orders: ${row.order_count} | Avg: £${Number(row.avg_order_value).toFixed(2)} | Failure: ${row.failure_rate_percent}%`);
    });

    // Worst performing restaurants  
    const worstPerformers = await db.query(`
      SELECT 
        r.name as restaurant_name,
        p.name as platform,
        COUNT(o.id) as order_count,
        COALESCE(SUM(o.order_value), 0) as total_revenue,
        ROUND(
          (COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT', 'CANCELLED_CUSTOMER', 'CANCELLED_RESTAURANT') THEN 1 END)::decimal / COUNT(o.id)) * 100, 2
        ) as failure_rate_percent
      FROM restaurants r
      JOIN platforms p ON r.platform_id = p.id
      LEFT JOIN orders o ON r.id = o.restaurant_id
      WHERE r.id IN (SELECT restaurant_id FROM orders)
      GROUP BY r.id, r.name, p.name
      HAVING COUNT(o.id) >= 5
      ORDER BY failure_rate_percent DESC, total_revenue ASC
      LIMIT 5
    `);

    console.log('\n🚨 Restaurants with highest failure rates (min 5 orders):');
    worstPerformers.rows.forEach((row: any, i: number) => {
      console.log(`  ${i+1}. ${row.restaurant_name} (${row.platform})`);
      console.log(`     Failure Rate: ${row.failure_rate_percent}% | Revenue: £${Number(row.total_revenue).toFixed(2)} | Orders: ${row.order_count}`);
    });

    // Platform analysis
    console.log('\n=== PLATFORM ANALYSIS ===');
    
    const platformStats = await db.query(`
      SELECT 
        p.name as platform,
        COUNT(r.id) as restaurant_count,
        COUNT(o.id) as order_count,
        COALESCE(AVG(o.order_value), 0) as avg_order_value,
        COALESCE(SUM(o.order_value), 0) as total_revenue,
        ROUND(
          (COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT', 'CANCELLED_CUSTOMER', 'CANCELLED_RESTAURANT') THEN 1 END)::decimal / NULLIF(COUNT(o.id), 0)) * 100, 2
        ) as platform_failure_rate
      FROM platforms p
      LEFT JOIN restaurants r ON p.id = r.platform_id
      LEFT JOIN orders o ON r.id = o.restaurant_id
      GROUP BY p.id, p.name
      ORDER BY total_revenue DESC
    `);

    platformStats.rows.forEach((row: any) => {
      console.log(`\n📱 ${row.platform}:`);
      console.log(`   Restaurants: ${row.restaurant_count} | Orders: ${row.order_count}`);
      console.log(`   Revenue: £${Number(row.total_revenue).toFixed(2)} | Avg Order: £${Number(row.avg_order_value).toFixed(2)}`);
      console.log(`   Platform Failure Rate: ${row.platform_failure_rate || 0}%`);
    });

    // Detailed insights for brand managers
    console.log('\n=== BRAND MANAGER INSIGHTS ===');
    
    // High-value outliers
    const outliers = await db.query(`
      SELECT 
        r.name as restaurant_name,
        p.name as platform,
        o.order_value,
        o.order_datetime,
        o.order_status
      FROM orders o
      JOIN restaurants r ON o.restaurant_id = r.id
      JOIN platforms p ON r.platform_id = p.id
      WHERE o.order_value > (
        SELECT AVG(order_value) + 2 * STDDEV(order_value) 
        FROM orders 
        WHERE order_value IS NOT NULL
      )
      ORDER BY o.order_value DESC
      LIMIT 5
    `);

    if (outliers.rows.length > 0) {
      console.log('\n💎 High-value order outliers (>2 std dev):');
      outliers.rows.forEach((row: any) => {
        console.log(`  ${row.restaurant_name} (${row.platform}): £${Number(row.order_value).toFixed(2)} on ${row.order_datetime?.toISOString().split('T')[0]} - ${row.order_status}`);
      });
    }

    // Time-based patterns
    const timePatterns = await db.query(`
      SELECT 
        EXTRACT(hour FROM order_datetime) as hour,
        COUNT(*) as order_count,
        AVG(order_value) as avg_value
      FROM orders 
      WHERE order_datetime IS NOT NULL
      GROUP BY EXTRACT(hour FROM order_datetime)
      ORDER BY hour
    `);

    console.log('\n⏰ Hourly order patterns:');
    timePatterns.rows.forEach((row: any) => {
      const hour = row.hour < 10 ? `0${row.hour}` : row.hour;
      const bar = '█'.repeat(Math.round(row.order_count / 20));
      console.log(`  ${hour}:00 ${bar} ${row.order_count} orders (£${Number(row.avg_value || 0).toFixed(2)} avg)`);
    });

    // Problem areas recommendations
    console.log('\n=== RECOMMENDATIONS ===');
    
    const problemAreas = await db.query(`
      SELECT 
        r.name as restaurant_name,
        p.name as platform,
        COUNT(o.id) as total_orders,
        COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT') THEN 1 END) as failed_orders,
        AVG(o.restaurant_wait_time_minutes) as avg_wait_time,
        AVG(o.prep_time_minutes) as avg_prep_time
      FROM restaurants r
      JOIN platforms p ON r.platform_id = p.id
      LEFT JOIN orders o ON r.id = o.restaurant_id
      WHERE r.id IN (SELECT restaurant_id FROM orders)
      GROUP BY r.id, r.name, p.name
      HAVING COUNT(o.id) >= 3 AND 
             (COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT') THEN 1 END)::decimal / COUNT(o.id)) > 0.2
      ORDER BY (COUNT(CASE WHEN o.order_status IN ('REJECTED', 'REJECTED_CUSTOMER', 'REJECTED_RESTAURANT') THEN 1 END)::decimal / COUNT(o.id)) DESC
      LIMIT 3
    `);

    if (problemAreas.rows.length > 0) {
      console.log('\n🔧 Action Required - High Failure Rate Restaurants:');
      problemAreas.rows.forEach((row: any, i: number) => {
        const failureRate = (row.failed_orders / row.total_orders * 100).toFixed(1);
        console.log(`\n${i+1}. ${row.restaurant_name} (${row.platform})`);
        console.log(`   ⚠️  Failure Rate: ${failureRate}% (${row.failed_orders}/${row.total_orders} orders)`);
        
        if (row.avg_wait_time > 15) {
          console.log(`   🕐 High wait times: ${Number(row.avg_wait_time).toFixed(1)} min avg`);
        }
        
        if (row.avg_prep_time > 20) {
          console.log(`   ⏱️  Long prep times: ${Number(row.avg_prep_time).toFixed(1)} min avg`);        }
        
      });
    }

    // Reviews and Ratings Analysis
    console.log('\n=== REVIEWS & RATINGS ANALYSIS ===');
    
    // Overall rating statistics
    const ratingStats = await db.query(`
      SELECT 
        COUNT(*) as total_ratings,
        AVG(rating_value) as avg_rating,
        MIN(rating_value) as min_rating,
        MAX(rating_value) as max_rating,
        COUNT(CASE WHEN rating_value >= 4.0 THEN 1 END) as high_ratings,
        COUNT(CASE WHEN rating_value <= 2.0 THEN 1 END) as low_ratings
      FROM ratings 
      WHERE rating_value IS NOT NULL
    `).then(result => result.rows[0]);

    if (ratingStats?.total_ratings > 0) {
      console.log('\n📊 Overall Rating Statistics:');
      console.log(`   Total Reviews: ${ratingStats.total_ratings}`);
      console.log(`   Average Rating: ${Number(ratingStats.avg_rating).toFixed(2)}/5.0`);
      console.log(`   Rating Range: ${Number(ratingStats.min_rating).toFixed(1)} - ${Number(ratingStats.max_rating).toFixed(1)}`);
      console.log(`   High Ratings (4+): ${ratingStats.high_ratings} (${((ratingStats.high_ratings / ratingStats.total_ratings) * 100).toFixed(1)}%)`);
      console.log(`   Low Ratings (2 or less): ${ratingStats.low_ratings} (${((ratingStats.low_ratings / ratingStats.total_ratings) * 100).toFixed(1)}%)`);
    }

    // Restaurant ratings (top and bottom)
    const restaurantRatings = await db.query(`
      SELECT 
        r.name as restaurant_name,
        p.name as platform,
        COUNT(rt.id) as review_count,
        AVG(rt.rating_value) as avg_rating
      FROM restaurants r
      JOIN platforms p ON r.platform_id = p.id
      JOIN ratings rt ON r.id = rt.restaurant_id
      WHERE rt.rating_value IS NOT NULL
      GROUP BY r.id, r.name, p.name
      HAVING COUNT(rt.id) >= 3
      ORDER BY AVG(rt.rating_value) DESC, COUNT(rt.id) DESC
      LIMIT 20
    `);

    if (restaurantRatings.rows.length > 0) {
      console.log('\n🏆 Top 3 Highest Rated Restaurants (min 3 reviews):');
      restaurantRatings.rows.slice(0, 3).forEach((row: any, i: number) => {
        const stars = '⭐'.repeat(Math.round(row.avg_rating));
        console.log(`  ${i+1}. ${row.restaurant_name} (${row.platform})`);
        console.log(`     ${stars} ${Number(row.avg_rating).toFixed(2)}/5.0 (${row.review_count} reviews)`);
      });

      console.log('\n⚠️  Bottom 3 Lowest Rated Restaurants (min 3 reviews):');
      restaurantRatings.rows.slice(-3).reverse().forEach((row: any, i: number) => {
        const stars = '⭐'.repeat(Math.round(row.avg_rating));
        console.log(`  ${i+1}. ${row.restaurant_name} (${row.platform})`);
        console.log(`     ${stars} ${Number(row.avg_rating).toFixed(2)}/5.0 (${row.review_count} reviews)`);
      });
    }

    // Rating distribution
    const ratingDistribution = await db.query(`
      WITH rating_ranges AS (
        SELECT 
          CASE 
            WHEN rating_value >= 4.5 THEN '4.5-5.0'
            WHEN rating_value >= 4.0 THEN '4.0-4.4'
            WHEN rating_value >= 3.5 THEN '3.5-3.9'
            WHEN rating_value >= 3.0 THEN '3.0-3.4'
            WHEN rating_value >= 2.5 THEN '2.5-2.9'
            WHEN rating_value >= 2.0 THEN '2.0-2.4'
            ELSE 'Below 2.0'
          END as rating_range,
          COUNT(*) as count
        FROM ratings 
        WHERE rating_value IS NOT NULL
        GROUP BY 
          CASE 
            WHEN rating_value >= 4.5 THEN '4.5-5.0'
            WHEN rating_value >= 4.0 THEN '4.0-4.4'
            WHEN rating_value >= 3.5 THEN '3.5-3.9'
            WHEN rating_value >= 3.0 THEN '3.0-3.4'
            WHEN rating_value >= 2.5 THEN '2.5-2.9'
            WHEN rating_value >= 2.0 THEN '2.0-2.4'
            ELSE 'Below 2.0'
          END
      )
      SELECT * FROM rating_ranges
      ORDER BY 
        CASE rating_range
          WHEN '4.5-5.0' THEN 1
          WHEN '4.0-4.4' THEN 2
          WHEN '3.5-3.9' THEN 3
          WHEN '3.0-3.4' THEN 4
          WHEN '2.5-2.9' THEN 5
          WHEN '2.0-2.4' THEN 6
          ELSE 7
        END
    `);

    if (ratingDistribution.rows.length > 0) {
      console.log('\n📈 Rating Distribution:');
      ratingDistribution.rows.forEach((row: any) => {
        const percentage = ((row.count / ratingStats.total_ratings) * 100).toFixed(1);
        const bar = '█'.repeat(Math.round((row.count / ratingStats.total_ratings) * 20));
        console.log(`  ${row.rating_range}: ${bar} ${row.count} (${percentage}%)`);
      });
    }

    // Platform and recent reviews
    const [platformRatings, recentReviews] = await Promise.all([
      db.query(`
        SELECT p.name as platform, COUNT(rt.id) as review_count, AVG(rt.rating_value) as avg_rating
        FROM platforms p
        LEFT JOIN restaurants r ON p.id = r.platform_id
        LEFT JOIN ratings rt ON r.id = rt.restaurant_id
        WHERE rt.rating_value IS NOT NULL
        GROUP BY p.id, p.name
        HAVING COUNT(rt.id) > 0
        ORDER BY AVG(rt.rating_value) DESC
      `),
      db.query(`
        SELECT r.name as restaurant_name, p.name as platform, rt.rating_value, rt.comment, rt.rating_date
        FROM ratings rt
        JOIN restaurants r ON rt.restaurant_id = r.id
        JOIN platforms p ON r.platform_id = p.id
        WHERE rt.rating_date IS NOT NULL
        ORDER BY rt.rating_date DESC
        LIMIT 5
      `)
    ]);

    if (platformRatings.rows.length > 0) {
      console.log('\n📱 Platform Rating Comparison:');
      platformRatings.rows.forEach((row: any) => {
        const stars = '⭐'.repeat(Math.round(row.avg_rating));
        console.log(`  ${row.platform}: ${stars} ${Number(row.avg_rating).toFixed(2)}/5.0 (${row.review_count} reviews)`);
      });
    }

    if (recentReviews.rows.length > 0) {
      console.log('\n🕒 Recent Reviews (Last 5):');
      recentReviews.rows.forEach((row: any, i: number) => {
        const stars = '⭐'.repeat(Math.round(row.rating_value));
        const date = row.rating_date?.toISOString().split('T')[0] || 'Unknown date';
        console.log(`  ${i+1}. ${row.restaurant_name} (${row.platform}) - ${date}`);
        console.log(`     ${stars} ${Number(row.rating_value).toFixed(1)}/5.0`);
        if (row.comment) {
          const truncatedComment = row.comment.length > 80 ? row.comment.substring(0, 80) + '...' : row.comment;
          console.log(`     "${truncatedComment}"`);
        }
      });
    }

  } catch (error: unknown) {
    Logger.error('Analysis failed', error as Error);
    throw new ProcessingError('Failed to complete analysis', { originalError: error });
  }
}

async function main() {
  try {
    const args = process.argv.slice(2);
    const command = args[0];

    if (!command) {
      console.log(`
📊 Order Data Management CLI

Usage:
  npx tsx cli.ts process <file> [integration]  - Process a CSV file
  npx tsx cli.ts analyse                        - Run comprehensive analysis

Examples:
  npx tsx cli.ts process "data.csv"
  npx tsx cli.ts process "data.csv" deliveryplatform3_total_order  
  npx tsx cli.ts analyse
`);
      process.exit(1);
    }

    // Validate command
    if (!Validators.isValidCommand(command)) {
      throw new ValidationError('Invalid command', { command, validCommands: ['process', 'analyse'] });
    }

    switch (command) {
      case 'process':
        const filePath = args[1];
        const integrationKey = args[2];
        
        if (!filePath) {
          throw new ValidationError('File path is required for process command');
        }
        
        await processFile(filePath, integrationKey);
        break;
        
      case 'analyse':
        await analyseOrders();
        break;
        
      default:
        throw new ValidationError('Unknown command', { command });
    }

    await db.close();
  } catch (error: unknown) {
    handleError(error, 'CLI execution');
  }
}

main().catch(error => {
  handleError(error, 'application startup');
}); 