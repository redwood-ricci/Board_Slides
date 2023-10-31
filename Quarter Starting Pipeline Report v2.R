source('ActiveCo Functions.R')
library(tidyr)
library(lubridate)
library(bigQueryR)
library(googlesheets4)

# sheet.link.v1 <- "https://docs.google.com/spreadsheets/d/1lyVuVVTAMhXE_ZBrjgqRTT6Xi490q3l8kzvDqbH32dw/edit#gid=984953587"
sheet.link <- "https://docs.google.com/spreadsheets/d/1IRqNj6u8OP0Pc1zfouOd5Hc-pEHSX3xn-IbD8aQxAzU/edit#gid=984953587"

# rpt.date is the date you want the report to reflect for Total Won & the end of the report
rpt.date <-   # as.Date('2023-09-30')# Sys.Date()
rpt.date <-   Sys.Date()
# snapshot anchor is the date the quarter starting pipline should start
# snapshot.anchor <- '2023-07-19' # '2023-10-11'
snapshot.anchor <- '2023-10-11'

# put opportunity ID numbers in this variable separated by a comma to exclude them from the report
exclude.ids <- c()
 
# this one below will exclude all Tidal
# exclude <- query.bq("select Id from skyvia.Opportunity where Legacy_Id__c = 'Legacy Tidal' OR Product__c = 'Tidal'")
# exclude.ids <- exclude$Id

# Below is a list of past snapshot dates used for the board meeting
# rpt.date <- as.Date('2022-12-31')
# snapshot.anchor <- '2022-10-10' # this is the date the starting pipeline is calculated
# rpt.date <- Sys.Date()#as.Date('2022-09-30')
# rpt.date <- as.Date('2023-03-31')
# snapshot.anchor <- '2023-01-12' # this is the date the starting pipeline is calculated
# snapshot.anchor <- '2022-09-12' # this is the date the starting pipeline is calculated
# rpt.date <- Sys.Date()# as.Date('2022-09-30')
# snapshot.anchor <- '2022-10-02' # this is the date the starting pipeline is calculated
# rpt.date <- as.Date('2023-06-30')
# snapshot.anchor <- '2023-04-14'

#################
q <- quarter(rpt.date,with_year = TRUE)
q.start.date <- floor_date(rpt.date,unit = 'quarter')   # this is quarter start based on rpt.date
q.end.date   <- ceiling_date(rpt.date,unit = 'quarter') # this is quarter end based on rpt.date

# make formated version of quarter for display in report
q.form <- as.character(q)
q.match <- gsub("\\.","",str_extract(q.form,"\\.."))
y.match <- str_extract(q.form,"^2...")
quarters.format <- paste0("Q",q.match,"-",y.match)


# first make a list of all the unique opps and the quarter they were forecasted in.
# requirements are opp has close date in quarter, is past discovery and has manager's forecast.

ff.tables <- query.bq("select * from activeco.Snapshots.INFORMATION_SCHEMA.TABLES")
# find the table that is closest to last Wednesday
# remove any tables that are not in standard date format
opp.tables <- ff.tables[which(grepl("^Opportunity_",ff.tables$table_name)),]
opp.tables$quarter <- quarter(opp.tables$snapshot_time_ms,with_year = TRUE)
# only opp tables after Q3
opp.tables <- opp.tables[which(opp.tables$snapshot_time_ms >= snapshot.anchor
                               & opp.tables$snapshot_time_ms < q.end.date),]
opp.tables <- opp.tables[order(opp.tables$snapshot_time_ms),]


####### Operationalize this section into it's own script to create the table in bigquery ######
for (t in 1:nrow(opp.tables)) {
  # t <- 1
  table <- opp.tables$table_name[t]
  print(paste0(t," of ",nrow(opp.tables)))
  print(table)
  forecast.opps <- query.bq(paste0("
  select
  o.Id,
  o.CloseDate,
  o.ACV_Bookings__c / ct.ConversionRate as qb_usd,
  o.Manager_s_Forecast__c / ct.ConversionRate as forecast_usd
  from `Snapshots.",table,"` o 
  left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
  where StageName not in ('Discovery','Demonstration','Untouched','Temporary','Data Quality','Closed Lost')
  and Test_Account__c = false
  and type in ('New Business','Existing Business')
  and CloseDate >= '",q.start.date,"'
  --and (Manager_s_Forecast__c > 0 OR (StageName = 'Closed Won'))
                                   "))
  
  forecast.opps$quarter <- q
  if (exists("forecasted.opps")) {
    forecasted.opps <- bind_rows(forecasted.opps,forecast.opps)
  }else{
    forecasted.opps <- forecast.opps
  }
  
}

rm(forecast.opps)
forecasted.opps$quarter <- quarter(forecasted.opps$CloseDate,with_year = TRUE)
forecasted.opps$CloseDate <- NULL
forecasted.opps <- forecasted.opps[which(!(forecasted.opps$Id %in% exclude.ids)),]

forecasted <- forecasted.opps %>%
  group_by(Id,quarter) %>%
  summarise(qb_usd = suppressWarnings(max(qb_usd,na.rm = T)), # the suppressWarnings is because some groups have no non missing arguments
            forecast_usd = suppressWarnings(max(forecast_usd,na.rm = T))
            )

forecasted.opps <- forecasted.opps[,c("Id","quarter")]
forecasted.opps <- forecasted.opps[!duplicated(forecasted.opps),]

# snapshot.anchor.format <- format(as.Date(snapshot.anchor),'%m/%d/%y')

# create the anchor table date in bigquery table format
anchor.bigquery.format <- format(as.Date(snapshot.anchor),"%Y%m%d")
print("Setup")
setup <- query.bq(paste0(
  "
create or replace table `temp.quarter_snap` as (
select
o.id,
o.Name,
o.Type,
o.SAO_Date__c,
o.AccountId,
o.StageName as previous_StageName,
o.CloseDate as previous_CloseDate,
ot.ACV_Bookings__c / ct.ConversionRate as QB_USD,
-- case when stagename = 'Closed Won' then 'Won' else 'Open' end as closed_won,
case when ot.region__c in ('North America','LATAM') then 'NA' else 'EU + ROW' end as Region_Bucket_Account
from
`Snapshots.Opportunity_",anchor.bigquery.format,"` o
left join `skyvia.CurrencyType` ct
on o.CurrencyIsoCode = ct.IsoCode
left join `skyvia.Opportunity` ot on o.id = ot.Id
-- left join `skyvia.User` u on o.OwnerId = u.Id
-- left join `R_Data.SAO_Dates` sao
-- on o.id = sao.OpportunityId
where o.type in ('New Business','Existing Business')
and o.StageName not in ('Closed Lost','Temporary','Data Quality')
and o.Test_Account__c = FALSE
and (o.Renewal_Type__c not in ('Annual Installment Billing','Prorated Installments','Annual Booking')or o.Renewal_Type__c is null)
and o.sao_date__c < '",snapshot.anchor,"'
and o.CloseDate >= '",q.start.date,"'
and o.CloseDate < '",q.end.date,"'
and o.OwnerId not in (
select Id from `skyvia.User` where Name in (
  'Kim Flint',
  'Ty Whitfield',
  'Michael Ricci',
  'Cecilia Saldarini',
  'Melissa McCaw',
  'Alex Lowe'
)
and o.id not in (",string.in.for.query(exclude.ids),")
)
);

create or replace table `temp.opps_today` as(
select
o.Id,
o.Name,
o.Type,
o.StageName as Current_StageName,
o.ACV_Bookings__c / ct.ConversionRate as Current_QB_USD,
o.CloseDate as Current_Closedate,
u.sales_territory__c as region_bucket_opp_owner, -- use sales territory as region bucket to assign by rep instead of company location
o.SAO_Date__c as SAO_Date__c,
o.Cohort_Close_Date__c,
case when o.region__c in ('North America','LATAM') then 'NA' else 'EU + ROW' end as Region_Bucket_Account
from `skyvia.Opportunity` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
left join `skyvia.User` u on o.OwnerId = u.Id
where Test_Account__c = FALSE
and StageName not in ('Discovery','Untouched','Temporary','Data Quality')
and (
CloseDate < '",q.end.date,"'
OR
o.Id in (select Id from `temp.quarter_snap`)
)
and o.OwnerId not in (
select Id from `skyvia.User` where Name in (
  'Kim Flint',
  'Ty Whitfield',
  'Michael Ricci',
  'Cecilia Saldarini',
  'Melissa McCaw',
  'Alex Lowe'
)
)
and o.id not in (",string.in.for.query(exclude.ids),")
);
select current_date() -- returns current date if successful
"
))

print("Starting Pipe")
starting.pipe <- query.bq(
  # this is saved as Snapshot Open Pipeline in Bigquery  
  "
select
tq.*,
o.current_StageName,
o.Current_QB_USD,
o.Current_Closedate,
o.region_bucket_opp_owner, -- use sales territory as region bucket to assign by rep instead of company location
coalesce(tq.Region_Bucket_Account,o.Region_Bucket_Account) as Merged_Account_Region_bucket,
from `temp.quarter_snap` tq
left join `temp.opps_today` o on o.id = tq.id
-- where Test_Account__c = FALSE
"
)

print("New Pipe")
new.pipe <- query.bq(paste0(
  "
select
o.Id,
o.Name,
o.Current_Closedate,
o.SAO_Date__c,
o.Region_Bucket_Account,
o.Type,
o.Current_QB_USD,
o.current_StageName
from `temp.opps_today` o
where o.SAO_Date__c >= '",q.start.date,"' and o.SAO_Date__c < '",q.end.date,"'
and Type in ('New Business','Existing Business')
and id not in (select id from `temp.quarter_snap`)
-- and Test_Account__c = FALSE
and o.Current_Closedate >= '",q.start.date,"'
and o.Current_Closedate < '",q.end.date,"'
"
))

names(new.pipe)[which(names(new.pipe) == "Region_Bucket_Account")] <- "Merged_Account_Region_bucket"

print("Pulled In")
pulled.in <- query.bq(paste0(
  "
-- pulled in:	Any SAO opp where New Value Close Date is in Quarter and Old Value is Greater than quarter
select distinct o.*
from `skyvia.OpportunityFieldHistory` fh
join `temp.opps_today` o on fh.OpportunityId = o.Id
where field = 'CloseDate'
and OldValue >= '",q.end.date,"'
and NewValue <  '",q.end.date,"'
and NewValue >=  '",q.start.date,"'
and OpportunityId not in (select id from `temp.quarter_snap`)
and o.Type in ('New Business','Existing Business')
and SAO_Date__c < '",q.end.date,"'
and o.Current_Closedate <= '",q.end.date,"'
and o.Current_Closedate >= '",q.start.date,"'
and o.id not in (",string.in.for.query(new.pipe$Id),")
-- and Test_Account__c = FALSE
"
))
names(pulled.in)[which(names(pulled.in) == "Region_Bucket_Account")] <- "Merged_Account_Region_bucket"
# make a list of all known pulled in opps before removing ones that were not forecasted
known.pulled.in <- unique(pulled.in$Id)
pulled.in <- pulled.in[which(pulled.in$Id %in% forecasted.opps$Id[which(forecasted.opps$quarter == q)]),]

print("Total Opps")
total.opps <- query.bq(paste0(
  "
select
o.Id,
o.Name,
o.Current_Closedate,
o.SAO_Date__c,
o.Type,
o.Region_Bucket_Account,
Current_QB_USD,
o.Current_StageName
from `temp.opps_today` o
where type in ('New Business','Existing Business')
and (Current_StageName = 'Closed Won' or SAO_Date__c is not null)
and Current_StageName not in ('Temporary','Data Quality')
and Cohort_Close_Date__c >= '",q.start.date,"'
and Cohort_Close_Date__c < '",q.end.date,"'
-- Test_Account__c = FALSE
"
))
names(total.opps)[which(names(total.opps) == "Region_Bucket_Account")] <- "Merged_Account_Region_bucket"

# by account region # all numbers are very close except for EMEA expansion looks high # total expansion looks OK


coverage.table <- starting.pipe %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(
    pipe = sum(Current_QB_USD,na.rm = T),
    
   `Pipeline Won` = sum(Current_QB_USD[which(current_StageName == 'Closed Won' & Current_Closedate < q.end.date)],na.rm = T),
            
    pushed = sum(Current_QB_USD[which(Current_Closedate >= q.end.date)],na.rm = T),
            
    lost = sum(Current_QB_USD[which(current_StageName == 'Closed Lost' & Current_Closedate < q.end.date)],na.rm = T)
  ) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

# calculate the pulled in
table.pulled.in <- pulled.in %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Pulled In` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

table.won.from.pulled.in <- pulled.in %>%
  filter(Current_StageName == 'Closed Won') %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Won From Pulled In` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

table.New.Pipe <- new.pipe %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`New Pipe` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

table.Won.New.Pipe <- new.pipe %>%
  filter(current_StageName == 'Closed Won') %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Won From New Pipe` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

# make a list of all the found opps so far
all.known.opp.ids <- unique(c(known.pulled.in,new.pipe$Id,starting.pipe$id))

table.found.opps <- total.opps %>%
  filter(!(Id %in% all.known.opp.ids)) %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Found` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

table.Won.found.opps <- total.opps %>%
  filter((!(Id %in% all.known.opp.ids)) & Current_StageName == 'Closed Won') %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Won From Found` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))

table.total.won <- total.opps %>%
  filter(Current_StageName == 'Closed Won') %>%
  group_by(Type,Merged_Account_Region_bucket) %>%
  summarise(`Total Won` = sum(Current_QB_USD,na.rm = T)) %>%
  arrange(desc(Type),desc(Merged_Account_Region_bucket))


share <- merge(coverage.table,table.pulled.in,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.New.Pipe,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.found.opps,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.total.won,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.won.from.pulled.in,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.Won.New.Pipe,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)
share <- merge(share,table.Won.found.opps,
               by = c("Type","Merged_Account_Region_bucket"),
               all = TRUE)

# Sana wants to lump 'Found' and Starting Pipe together
share$pipe <- rowSums(share[,c("pipe","Found")],na.rm = T)
share$`Pipeline Won` <- rowSums(share[,c("Pipeline Won","Won From Found")],na.rm = T)

share$Found <- NULL
share$`Won From Found` <- NULL

# col.order <- c("pipe","Pipeline Won","Pulled In","New Pipe","Total Won","pushed","lost")
col.order <- c("pipe","New Pipe","Pulled In",
               # "Found",
               "pushed","Total Won","Pipeline Won","Won From Pulled In","Won From New Pipe",
               # "Won From Found",
               "lost")
share <- share[,c("Type","Merged_Account_Region_bucket",col.order)]

share.format <- share

# for (v in col.order) {
#   share.format[,v] <- format.money(share.format[,v])
# }


share.format <- share.format[order(share.format$Type,share.format$Merged_Account_Region_bucket,decreasing = TRUE),]
new.share.format <- share.format[which(share.format$Type == "New Business"),]
new.share.format$Type <- NULL
expansion.share.format <- share.format[which(share.format$Type == "Existing Business"),]
expansion.share.format$Type <- NULL
# paste.excel(share.format)

starting.pipe$Name <- asci.hyperlinks(starting.pipe$id,starting.pipe$Name)

range_write(sheet.link,
            as.data.frame(quarters.format),
            range = "A1",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

range_write(sheet.link,
            as.data.frame(snapshot.anchor),
            range = "A2",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

range_write(sheet.link,
            new.share.format,
            range = "B5",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

range_write(sheet.link,
            expansion.share.format,
            range = "B10",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

range_write(sheet.link,
            as.data.frame(snapshot.anchor),
            range = "A2",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

# merge the found opps with the starting pipeline
total.opps$Name <- asci.hyperlinks(total.opps$Id,total.opps$Name)

found <- total.opps[which(!(total.opps$Id %in% all.known.opp.ids)),]
names(found)[which(names(found) == 'Id')] <- "id"
found$previous_StageName <- 'Found'
found <- found[,which(names(found) %in% names(starting.pipe))]
starting.pipe <- bind_rows(starting.pipe,found)

# write_sheet(found,sheet.link,
#             sheet = 'Found Opps')

write_sheet(starting.pipe,sheet.link,
            sheet = 'Starting Pipeline')

pulled.in$Name <- asci.hyperlinks(pulled.in$Id,pulled.in$Name)
write_sheet(pulled.in,sheet.link,
            sheet = 'Pulled In')

new.pipe$Name <- asci.hyperlinks(new.pipe$Id,new.pipe$Name)
write_sheet(new.pipe,sheet.link,
            sheet = 'New Pipe Created')

total.won <- total.opps[which(total.opps$Current_StageName == 'Closed Won'),]
write_sheet(total.won,sheet.link,
            sheet = 'Total Won')

est_now <- with_tz(Sys.time(), "America/New_York")
range_write(sheet.link,
            as.data.frame(paste0("Last Refreshed: ",est_now)),
            range = "B27",
            col_names = FALSE,
            sheet = 'Summary',
            reformat = FALSE)

# paste.excel(share.format)
# plus <- unique(c(starting.pipe$id,new.pipe$Id,pulled.in$Id))
# pushed.ids <- starting.pipe$id[which(starting.pipe$Current_Closedate >= q.end.date)]
# lost.new.ids <- new.pipe$Id[which(new.pipe$current_StageName == 'Closed Lost')]
# minus <- unique(c(pushed.ids,lost.new.ids))
# 
# new.pipe.inflation.adjust <- starting.pipe$QB_USD[which(starting.pipe$current_StageName == 'Closed Won')] - 
#   starting.pipe$Current_QB_USD[which(starting.pipe$current_StageName == 'Closed Won')]
# 
# table(total.opps$Id %in% c(plus,minus))



# check out pipe by owner region instead # looks like Geo region is more accurate to OG table
# by owner region
# pipe %>%
#   group_by(region_bucket_opp_owner,Type) %>%
#   summarise(open_or_won = sum(QB_USD,na.rm = T)) %>%
#   arrange(desc(Type),desc(region_bucket_opp_owner))

# diagnose issue
# won.ids <- total.won$Id
# # View(pulled.in[which(!(pulled.in$Id %in% won.ids)),])
# 
# pulled.in.won.ids <- pulled.in$Id[which(pulled.in$Current_StageName == 'Closed Won')]
# new.won.ids <- new.pipe$Id[which(new.pipe$current_StageName == 'Closed Won')]
# starting.won.ids <- starting.pipe$id[which(starting.pipe$current_StageName == 'Closed Won')]
# 
# cat.won.ids <- unique(c(pulled.in.won.ids,new.won.ids,starting.won.ids))
# table(cat.won.ids %in% won.ids)
# table(won.ids %in% cat.won.ids)
# won.ids[!(won.ids %in% cat.won.ids)]
# 
# table(pulled.in.won.ids %in% total.won$Id)
# table(pulled.in.won.ids %in% starting.won.ids)
# table(pulled.in.won.ids %in% new.won.ids)
# 
# table(starting.won.ids %in% total.won$Id)
# table(starting.won.ids %in% pulled.in.won.ids)
# table(starting.won.ids %in% new.won.ids)
# 
# table(new.won.ids %in% total.won$Id)
# table(new.won.ids %in% pulled.in.won.ids)
# table(new.won.ids %in% starting.won.ids)
# 
# table(new.pipe$Id %in% pulled.in$Id)


