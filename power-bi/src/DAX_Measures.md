## List of DAX measures used on Data visualizations in Mirosoft Power BI

### Total gross revenue paid by the customer.
TotalRevenue = SUM('fact_trips'[total_amount])

### Total audited cost recovered for tolls and fees.
Total Tolls Amount = SUM('fact_trips'[tolls_amount])

### Total count of all completed trips.
TotalTrips = COUNTROWS('fact_trips')

### Average time spent on valid trips in minutes.
Trip Duration = AVERAGEX('fact_trips', 
                DATEDIFF(fact_trips[pickup_datetime], 'fact_trips'[dropoff_datetime], MINUTE))

### Trip volume using the _inactive_ Dropoff relationship.
TripsByDropoffLocation = CALCULATE([TotalTrips], 
                    USERELATIONSHIP(dim_location[location_id], fact_trips[do_location_id]))

### Revenue using the _inactive_ Dropoff relationship.
RevenueByDropoffLocation = CALCULATE([TotalRevenue], 
                    USERELATIONSHIP(dim_location[location_id], fact_trips[do_location_id]))

### Mean gross payment per trip.
AverageTripFare = DIVIDE([TotalRevenue], [TotalTrips])

### Mean distance traveled per trip.
AverageTripDistance = AVERAGE(fact_trips[trip_distance])

### Tip amount relative to fare.
AverageTip = DIVIDE(
    SUM(fact_trips[tip_amount]), 
    SUM(fact_trips[fare_amount]))

### Revenue Yield_ (dollars generated per mile)
AverageMileRevenue = DIVIDE([TotalRevenue], SUM(fact_trips[trip_distance]))