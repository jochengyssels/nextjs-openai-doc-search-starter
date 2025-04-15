async def main():
    # Initial seed URLs focused on kitesurfing spots
    seed_urls = [
        "https://www.kitesurfing-handbook.com/kitesurf-spots/",
        "https://www.kitesurfingadvisor.com/kitesurf-spots.html",
        "https://www.kiteboarder.com/kiteboarding-spots/",
        "https://www.iksurf.com/kite-spots/"
    ]
    
    # Create crawler and processor
    crawler = ModernWebCrawler(max_concurrency=15)
    processor = KitespotDataProcessor()
    
    # Run initial crawl
    print("Starting initial crawl...")
    results = await crawler.run(seed_urls, max_pages=50)
    
    # Process results
    processing_results = processor.process_results(results)
    print(f"Initial crawl complete. Found {processing_results['new_kitespots']} new kitespots.")
    
    # Continuous learning loop
    while True:
        # Generate new seed URLs based on discovered kitespots
        new_seeds = []
        
        # Find related websites from existing kitespots
        for kitespot in processor.kitespots[-20:]:  # Use most recent discoveries
            # Extract domain from URL
            url_parts = kitespot["url"].split("/")
            domain = f"{url_parts[0]}//{url_parts[2]}"
            
            # Add domain homepage if not already in seeds
            if domain not in new_seeds and domain not in seed_urls:
                new_seeds.append(domain)
                
        # Add geographic variations based on discovered locations
        for kitespot in processor.kitespots:
            for location in kitespot["location"]:
                if location != "Unknown":
                    search_url = f"https://www.google.com/search?q=kitesurfing+{location.replace(' ', '+')}"
                    if search_url not in new_seeds and search_url not in seed_urls:
                        new_seeds.append(search_url)
        
        # Run incremental crawl with new seeds
        print(f"Starting incremental crawl with {len(new_seeds)} new seed URLs...")
        results = await crawler.run(new_seeds, max_pages=30)
        
        # Process new results
        processing_results = processor.process_results(results)
        print(f"Incremental crawl complete. Found {processing_results['new_kitespots']} new kitespots.")
        
        # Update seed URLs for next iteration
        seed_urls.extend(new_seeds)
        
        # Wait before next crawl cycle
        print("Waiting for next crawl cycle...")
        await asyncio.sleep(3600)  # Wait 1 hour between crawls

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
