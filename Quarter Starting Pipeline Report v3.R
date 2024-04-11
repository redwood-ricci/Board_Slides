# source('Update Forecasted Opps Table in Bigquery.R')
source('ActiveCo Functions.R')
library(tidyr)
library(lubridate)
library(bigQueryR)
library(googlesheets4)

sheet.link <- "https://docs.google.com/spreadsheets/d/1w5J_iSXnEt2ZXopvsbbdIMvJZ3eLWDeZY51sRb0kaUs/edit#gid=0"

# Q1
# rpt.date <-    as.Date('2023-12-31')
# snapshot.anchor <- '2023-10-11'

# Q2
# rpt.date <-    as.Date('2023-06-30')
# snapshot.anchor <- '2023-04-14'

# Q3 
# rpt.date is the date you want the report to reflect for Total Won & the end of the report
#rpt.date <- as.Date('2023-11-27')
# rpt.date <- Sys.Date()
# snapshot.anchor <- '2023-04-10'
# snapshot.anchor = last pipeline meeting start date

rpt.date <-    as.Date('2023-09-30')
snapshot.anchor <- '2023-07-19'

# # Q4
# rpt.date <-    Sys.Date()
# snapshot.anchor <- '2023-10-11'
# snapshot anchor is the date the quarter starting pipline should start

# Q1
rpt.date <-    as.Date('2024-03-31')
snapshot.anchor <- '2024-01-09'
# snapshot anchor is the date the quarter starting pipline should start


# put opportunity ID numbers in this variable separated by a comma to exclude them from the report
# exclude.ids <- c('0063t000013UwmhAAC')
q <- quarter(rpt.date,with_year = TRUE)
q.start.date <- floor_date(rpt.date,unit = 'quarter')   # this is quarter start based on rpt.date
q.end.date   <- ceiling_date(rpt.date,unit = 'quarter') # this is quarter end based on rpt.date

# make formated version of quarter for display in report
q.form <- as.character(q)
q.match <- gsub("\\.","",str_extract(q.form,"\\.."))
y.match <- str_extract(q.form,"^2...")
quarters.format <- paste0("Q",q.match,"-",y.match)

starting.pipeline <- query.bq(
  paste0(
    "
select 
h.Id,
h.Warboard_Category__c,
h.Type,
o.Region__c,
o.Account_Segment__c,
o.Product__c,
round(coalesce(cast(h.Software_USD as int64),cast(h.Expansion_USD as int64),cast(h.qb_usd as int64)),2) as QB_USD,
o.StageName,
o.CloseDate,
o.LeadSource,
'Starting Pipe' as Mike_Type
from `R_Data.Opportunity_History` h
left join `skyvia.Opportunity` o on h.Id = o.Id
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
where h.StageName not in ('Temporary','Data Quality')
and h.type in ('New Business','Existing Business','Expansion')
and cast(Snapshot_Time as date) = '",snapshot.anchor,"'
and o.Test_Account__c = false
and o.SAO_Date__c <= '",snapshot.anchor,"'
and h.CloseDate >= '",q.start.date,"'
and h.CloseDate < '",q.end.date,"'
    "
  )
)

make.geo <- function(x){
  x$Geo <- 'EMEA + ROW'
  x$Geo[which(x$Region__c %in% c('North America','LATAM'))] <- "North America"
  return(x)
}

starting.pipeline <- make.geo(starting.pipeline)

# ghost.starting.pipeline <- query.bq(
#   paste0(
#     "
# select 
# o.Id,
# o.Warboard_Category__c,
# o.Type,
# o.Region__c,
# o.Account_Segment__c,
# o.Product__c,
# qb_usd as QB_USD,
# o.StageName,
# o.CloseDate,
# o.LeadSource,
# 'Starting Pipe' as Mike_Type
# from `R_Data.Opportunity_History` h
# left join `skyvia.Opportunity` o on h.Id = o.Id
# where o.StageName not in ('Identify','Discovery','Demonstration','Untouched','Temporary','Data Quality')
# and o.type in ('New Business','Existing Business')
# and cast(Snapshot_Time as date) = '",snapshot.anchor,"'
# and o.Test_Account__c = false
# and o.SAO_Date__c <= '",snapshot.anchor,"'
# and h.CloseDate >= '",q.start.date,"'
# and h.CloseDate < '",q.end.date,"'
# and h.Id not in (",string.in.for.query(starting.pipeline$Id),")
#     "
#   )
# )
# 
# ghost.starting.pipeline <- make.geo(ghost.starting.pipeline)
# 
# starting.pipeline <- bind_rows(starting.pipeline,ghost.starting.pipeline)

# starting.pipeline %>%
#   group_by(Geo,Type) %>%
#   summarise(sum(qb_usd,na.rm = T))

pulled.in <- query.bq(paste0(
  "
-- pulled in:	Any SAO opp where New Value Close Date is in Quarter and Old Value is Greater than quarter
select 
distinct
o.Id,
o.Warboard_Category__c,
max(cast(h.CreatedDate as DATE)) as moved_date,
case when max(cast(h.CreatedDate as DATE)) >= o.CloseDate and o.StageName = 'Closed Lost' then 'Clean Up Pulled In'
when max(h.CreatedDate) < '",snapshot.anchor,"' then 'Clean Up Pulled In' else 'Pulled In' end as clean,
o.Type,
o.Region__c,
o.Account_Segment__c,
o.Product__c,
max(coalesce(o.Software_Qualified_Bookings__c,o.RW_Expansion_Amount__c) / ct.ConversionRate) as QB_USD,
o.StageName,
o.CloseDate,
o.LeadSource,
'Pulled In' as Mike_Type
from `skyvia.Opportunity` o
-- from `Snapshots.Opportunity_20231030` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
left join `skyvia.OpportunityFieldHistory` h on h.OpportunityId = o.Id
where o.StageName not in ('Temporary','Data Quality') 
and o.type in ('New Business','Existing Business','Expansion')
and o.Test_Account__c = false
and o.SAO_Date__c < '",q.end.date,"'
and field = 'CloseDate'
and OldValue >= '",q.end.date,"'
and NewValue <  '",q.end.date,"'
and NewValue >=  '",q.start.date,"'
and o.CreatedDate < '",q.end.date,"'
-- and CloseDate <= '",q.end.date,"'
and CloseDate >= '",q.start.date,"'
and CloseDate < '",q.end.date,"'
and o.id not in (",string.in.for.query(starting.pipeline$Id),")
group by 1,2,5,6,7,8,10,11,12,13
"
))

pulled.in$Mike_Type <- pulled.in$clean
pulled.in$moved_date <- NULL
pulled.in$clean <- NULL
pulled.in <- make.geo(pulled.in)
# used for making an adjustment to pulled in
# pulled.in$QB_USD[which(pulled.in$Type == 'New Business')] <- round(pulled.in$QB_USD[which(pulled.in$Type == 'New Business')]*0.9002074342,2)
# pulled.in$QB_USD[which(pulled.in$Type == 'Existing Business')] <- round(pulled.in$QB_USD[which(pulled.in$Type == 'Existing Business')]*0.656999057,2)


new.pipe <- query.bq(paste0(
  "
select 
o.Id,
o.Warboard_Category__c,
o.Type,
o.Region__c,
o.Account_Segment__c,
o.Product__c,
coalesce(o.Software_Qualified_Bookings__c,o.RW_Expansion_Amount__c) / ct.ConversionRate as QB_USD,
o.StageName,
o.CloseDate,
o.LeadSource,
'New Pipe' as Mike_Type
from `skyvia.Opportunity` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
where o.StageName not in ('Temporary','Data Quality')
and o.type in ('New Business','Existing Business','Expansion')
and o.Test_Account__c = false
and o.SAO_Date__c >= '",q.start.date,"'
and o.SAO_Date__c < '",q.end.date,"'
and o.CloseDate >= '",q.start.date,"'
and o.CloseDate < '",q.end.date,"'
and o.id not in (",string.in.for.query(starting.pipeline$Id),")
and o.id not in (",string.in.for.query(pulled.in$Id),")
"
))

new.pipe <- make.geo(new.pipe)


pushed <- query.bq(paste0(
  "
select 
o.Id,
o.Warboard_Category__c,
o.Type,
o.Region__c,
o.Account_Segment__c,
o.Product__c,
coalesce(o.Software_Qualified_Bookings__c,o.RW_Expansion_Amount__c) / ct.ConversionRate as QB_USD,
o.StageName,
o.CloseDate,
o.LeadSource,
'Pushed' as Mike_Type
from `skyvia.Opportunity` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
where o.StageName not in ('Temporary','Data Quality')
and o.type in ('New Business','Existing Business','Expansion')
and o.Test_Account__c = false
and o.SAO_Date__c <= '",snapshot.anchor,"'
and o.CloseDate > '",q.end.date,"'
and o.id in (",string.in.for.query(c(starting.pipeline$Id)),")
"
)) # fix for starting pipe pushed

pushed <- make.geo(pushed)

total.opps <- query.bq(paste0(
  "
select 
distinct
o.Id,
o.Warboard_Category__c,
o.Type,
o.Region__c,
o.Account_Segment__c,
o.Product__c,
coalesce(o.Software_Qualified_Bookings__c,o.RW_Expansion_Amount__c) / ct.ConversionRate as QB_USD,
o.StageName,
o.CloseDate,
o.LeadSource,
'Total' as Mike_Type
from `skyvia.Opportunity` o
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
where (o.StageName = 'Closed Won' OR SAO_Date__c is not null)
and o.StageName not in ('Temporary','Data Quality')
and o.type in ('New Business','Existing Business','Expansion')
and o.Test_Account__c = FALSE
and o.CloseDate >= '",q.start.date,"'
and o.CloseDate < '",q.end.date,"'
"
))

total.opps <- make.geo(total.opps)

all.known.opps <- unique(c(pulled.in$Id,new.pipe$Id,starting.pipeline$Id))

ghost.found <- total.opps[which(!(total.opps$Id %in% all.known.opps)),]
ghost.found$Mike_Type <- 'New Pipe'
if(nrow(ghost.found) > 0){
  warning('Ghost opps found in total opps')
}
starting.pipeline <- bind_rows(starting.pipeline,ghost.found)


seed <- bind_rows(new.pipe,pulled.in,pushed,starting.pipeline,total.opps)

dups <- seed$Id[duplicated(seed$Id)]
no.dups <- seed$Id[which(!(seed$Id %in% dups))]
seed$Id[seed$Id %in% no.dups & seed$StageName == 'Closed Won']
seed$Id[seed$Id %in% no.dups & seed$StageName == 'Closed Won']
# View(seed[which(seed$Id %in% no.dups),])

new.ids <- unique(new.pipe$Id)
pulled.in.ids <- unique(pulled.in$Id)
pushed.ids <- unique(pushed$Id)
starting.ids <- unique(starting.pipeline$Id)

# check won totals
total.opps %>%
  group_by(Warboard_Category__c) %>%
  filter(StageName == 'Closed Won') %>%
  summarise(won = sum(QB_USD,na.rm = T),
            num = length(Id))


enrich.data <- query.bq(paste0(
  "
  select
o.id as Id,
ur.Name as User_Role,
case when lower(ur.Name) like '%customer%' then 'CAM'
     when lower(ur.Name) like '%strategic%' then 'SAM'
     else 'Other' end as CAM_or_SAM
from `skyvia.Opportunity` o
left join `skyvia.User` u on o.OwnerId = u.Id
left join `skyvia.UserRole` ur on u.UserRoleId = ur.Id
where o.id in (
",string.in.for.query(seed$Id),"  
)"
))
seed <- merge(seed,enrich.data,by = 'Id',all.x = T)

# done to mark the Expansion children as Existing Business
seed$Type_Smooth <- seed$Type
seed$Type_Smooth[which(seed$Type_Smooth == 'Expansion')] <- 'Existing Business'
# share.product <- seed %>%
#   group_by(Geo,Warboard_Category__c,Product__c) %>%
#   summarise(`Starting Pipeline` = sum(QB_USD[which(Mike_Type == 'Starting Pipe')],na.rm = T),
#             `New Pipe` = sum(QB_USD[which(Mike_Type == 'New Pipe')],na.rm = T),
#             `Pulled In` = sum(QB_USD[which(Mike_Type == 'Pulled In')],na.rm = T),
#             `Pushed` = sum(QB_USD[which(Mike_Type == 'Pushed')],na.rm = T),
#             `Total Won` = sum(QB_USD[which(Mike_Type == 'Total' & StageName == 'Closed Won')],na.rm = T),
#             `Starting Pipeline Won` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% starting.ids)],na.rm = T),
#             `Won From Pulled In` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% pulled.in.ids)],na.rm = T),
#             `Won From New Pipe` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% new.ids)],na.rm = T),
#             `Starting Pipe Lost` = sum(QB_USD[which(Mike_Type == 'Starting Pipe' & StageName == 'Closed Lost')],na.rm = T),
#             `Coverage of Closed Won` = `Starting Pipeline` / `Total Won`
#             ) %>%
#   arrange(Warboard_Category__c,Geo,Product__c)
# 
# share.product$`Coverage of Closed Won`[!is.finite(share.product$`Coverage of Closed Won`)] <- NA
# 
# write_sheet_keep_sheet_format(share.product,
#             sheet.link,
#             'Outcome')

share.total <- seed %>%
  group_by(Geo,Warboard_Category__c,Product__c,Account_Segment__c,LeadSource,Type_Smooth,
           CAM_or_SAM) %>%
  summarise(`Starting Pipeline` = sum(QB_USD[which(Mike_Type == 'Starting Pipe')],na.rm = T),
            `New Pipe` = sum(QB_USD[which(Mike_Type == 'New Pipe')],na.rm = T),
            `Pulled In` = sum(QB_USD[which(Mike_Type == 'Pulled In')],na.rm = T),
            `Pushed` = sum(QB_USD[which(Mike_Type == 'Pushed')],na.rm = T),
            `Total Won` = sum(QB_USD[which(Mike_Type == 'Total' & StageName == 'Closed Won')],na.rm = T),
            `Starting Pipeline Won` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% starting.ids & Mike_Type == 'Total'
                                                       & CloseDate <= q.end.date)],na.rm = T),
            `Won From Pulled In` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% pulled.in.ids & Mike_Type == 'Total')],na.rm = T),
            `Won From New Pipe` = sum(QB_USD[which(StageName == 'Closed Won' & Id %in% new.ids & Mike_Type == 'Total')],na.rm = T),
            `Starting Pipeline Lost` = sum(QB_USD[which(StageName == 'Closed Lost' & Id %in% starting.ids & Mike_Type == 'Total'
                                                        & CloseDate <= q.end.date)],na.rm = T),
            `Coverage of Closed Won` = `Starting Pipeline` / `Total Won`
  ) %>%
  arrange(Warboard_Category__c,Geo,Product__c)

share.total$`Coverage of Closed Won`[!is.finite(share.total$`Coverage of Closed Won`)] <- NA

write_sheet_keep_sheet_format(share.total,
                              sheet.link,
                              'Pivot Data')

write_sheet_keep_sheet_format(seed,
                              sheet.link,
                              'Raw Data')

range_write(sheet.link,
            as.data.frame(quarters.format),
            'Outcome Pivot',
            'A1',
            col_names = FALSE,
            reformat = FALSE
)
range_write(sheet.link,
            as.data.frame(snapshot.anchor),
            'Outcome Pivot',
            'A2',
            col_names = FALSE,
            reformat = FALSE
)
range_write(sheet.link,
            as.data.frame(Sys.time()),
            'Outcome Pivot',
            'a4',
            col_names = FALSE,
            reformat = FALSE
)


