source('ActiveCo Functions.R')
library(tidyr)
library(lubridate)
library(googlesheets4)
sf_auth(cache = ".httr-oauth-salesforcer-asci") # authorize connection

# change the quarter here if desired
rpt.date <- Sys.Date()
# rpt.date <- as.Date('2022-10-01')
# rpt.id <- "00O3t000008a9yqEAA"
sheet.link <- "https://docs.google.com/spreadsheets/d/1ouTj1K3UGfgzmwxZWfsZX6nEJfbkWxU9rxa8Va9h9-k/edit#gid=0"

# grade reps by quarterly performance or yearly performance
rpt.compare <- "Year"
# rpt.compare <- "Quarter"

# start of prior year
rpt.start <- floor_date(floor_date(rpt.date, unit = 'year')-1,unit = 'year') # one quarter ago
# get opps from next quarter
rpt.end <- ceiling_date(ceiling_date(rpt.date, unit = 'quarter')+1,unit = 'quarter') # one quarter in the future

past.quarter.start <- floor_date(floor_date(rpt.date, unit = 'quarter')-1,unit = 'quarter') # one quarter ago
past.quarter.end <- ceiling_date(past.quarter.start,unit = 'quarter')

this.quarter.start <- floor_date(rpt.date,unit = 'quarter')
this.quarter.end <- ceiling_date(rpt.date,unit = 'quarter')-1

next.quarter.start <- floor_date(this.quarter.end + 1,unit = 'quarter')
next.quarter.end <- ceiling_date(this.quarter.end + 1,unit = 'quarter')

last.year.start <- rpt.start
last.year.end <- ceiling_date(rpt.start,'year')

if(rpt.compare == 'Year'){
  unit.start <- last.year.start
  unit.end   <- last.year.end
}else{
  # unit.start <- last
  # unit.end   <- last.quar
}

# can manually set the unit start and end
# unit.start <- '2022-07-01'
# unit.end   <- '2022-07-01'

# variables for this quarter and next quarter
tq <- quarter(this.quarter.start,with_year = TRUE) # this quarter
nq <- quarter(next.quarter.start,with_year = TRUE) # next quarter
pq <- quarter(past.quarter.start,with_year = TRUE) # previous quarter

# pretty quarter formating for report out
tq.format <- paste0("Q",quarter(this.quarter.start))# paste0("Q",quarter(rpt.date)," ",format(rpt.date,"%Y"))
nq.format <- paste0("Q",quarter(next.quarter.start))# paste0("Q",quarter(rpt.end-1)," ",format(rpt.end-1,"%Y"))

# update report for prior quarter and next quarter based on report date
opps <- query.bq(paste0(
  "select 
o.Id as OppId,
a.Id as AccountId,
a.Name as Account_Name,
u.Name as Owner_Name,
ur.Name as User_Role,
o.Name as Opp_Name,
o.StageName as Stage,
o.Fiscal as Fiscal_Period,
o.Amount / ct.ConversionRate as Amount,
o.ExpectedRevenue / ct.ConversionRate as Expected_Revenue,
-- o.Age,
o.Days_Stale__c as Days_Stale,
o.CloseDate as Close_Date,
o.CreatedDate as Created_Date,
o.NextStep as Sales_Next_Steps,
o.LeadSource as Lead_Source,
o.Type,
o.Cohort_Close_Date__c as Cohort_Close_Date,
o.Owner_Role_at_Assignment__c,
o.ACV_Bookings__c / ct.ConversionRate as Qualified_Bookigns,
o.Existing_Contract_End_Date__c,
o.Warboard_Category__c,
o.Recurring_Booking_PY__c / ct.ConversionRate as Recurring_Booking_PY,
o.Manager_s_Forecast__c / ct.ConversionRate as Managers_Forecast,
o.SAO_Date__c,
o.MEDDPICC_Score__c,
o.Billing_Period_1_Amount__c / ct.ConversionRate as Billing_Period_1,
from `skyvia.Opportunity` o
left join `skyvia.Account` a on o.AccountId = a.Id
left join `skyvia.User` u on o.OwnerId = u.Id
left join `skyvia.UserRole` ur on u.UserRoleId = ur.Id
left join `skyvia.CurrencyType` ct on o.CurrencyIsoCode = ct.IsoCode
where o.Cohort_Close_Date__c >='",rpt.start,"'
and o.Cohort_Close_Date__c <'",rpt.end,"'
and o.Test_Account__c = FALSE
and (Renewal_Type__c not in ('Annual Installment Billing','Prorated Installments','Annual Booking') or Renewal_Type__c is null)
and (u.Name not in ('Kim Flint','Ty Whitfield','Michael Ricci','Cecilia Saldarini','Melissa McCaw','Alex Lowe') or u.Name is null)
and o.stagename not in ('Temporary','Data Quality')"
))

opps$rpt_qtr <- quarter(opps$Cohort_Close_Date,with_year = TRUE)

# make links
opps$Account_Name_link <- asci.hyperlinks(opps$AccountId,opps$Account_Name)
opps$Opp_Name_link <- asci.hyperlinks(opps$OppId,opps$Opp_Name)

# replace NA days stale with Redwood migration date
opps$Days_Stale[which(is.na(opps$Days_Stale))] <- difftime(Sys.Date(),as.Date('2022-02-05'))

# create a table for summary scorecard slide
flips <- c("Flips - Maintenance","Flips - Term License")
# for the sake of this all flips + renewals go together
renewals <- c("Renewal",flips) # flips,

# cams <- c("Mike Ries","Kevin Yost","Loren Staselewicz")
opps$won <- FALSE
opps$won[which(opps$Stage == 'Closed Won')] <- TRUE
opps$lost <- FALSE
opps$lost[which(opps$Stage == 'Closed Lost')] <- TRUE
opps$open <- FALSE
opps$open[which(opps$won == FALSE & opps$lost == FALSE)] <- TRUE
# make forecast 0, QB for won deals and manager's forecast for open deals
opps$forecast <- 0
opps$forecast[which(opps$won == T)] <- opps$Qualified_Bookigns[which(opps$won == T)]
opps$forecast[which(opps$open == T)] <- opps$Managers_Forecast[which(opps$open == T)]

# calculation for standard dollar retention numberator
# taking the minimum value of the columns listed below
opps$sdr.numerator <- apply(opps[,c("forecast","Billing_Period_1","Recurring_Booking_PY")], 1, min,na.rm = T)
opps$sdr.numerator[which(is.infinite(opps$sdr.numerator))] <- NA
 

for(p in c("Y","Q")){
# p <- "Y"
if(p == 'Y'){
  p.start <- unit.start
  p.end <- unit.end
  
  cam.scorecard.raw <- opps %>%
    filter(Cohort_Close_Date >= p.start & # here is to change the date range
             Cohort_Close_Date < p.end &
             Warboard_Category__c %in% renewals) %>% 
    group_by(Owner_Name)
  
  write_sheet(cam.scorecard.raw,sheet.link,
              sheet = 'raw_cam_prior_year')
}else{
  p.start <- past.quarter.start
  p.end <- past.quarter.end
}

# write the CAM scorecards
cam.scorecard.renewals <- opps %>%
  filter(Cohort_Close_Date >= p.start & # here is to change the date range
           Cohort_Close_Date < p.end &
           Warboard_Category__c %in% renewals) %>% 
  group_by(Owner_Name)%>%
  summarise(
    # total won $
    Renew.avail.Num =    length(Warboard_Category__c),
    Renew.Won =          length(Warboard_Category__c[which(won == T)]),
    Renew.Lost =         length(Warboard_Category__c[which(lost == T)]),
    Renew.avail.usd =    sum(Recurring_Booking_PY,na.rm = T),
    Renew.Won.QB.usd =   sum(Qualified_Bookigns[which(won == T)],na.rm = T),
    Renew.Won.RBPY.usd = sum(Recurring_Booking_PY[which(won == T)],na.rm = T),
    gross.pim =          Renew.Won.QB.usd / Renew.Won.RBPY.usd,
    retention =          round(Renew.Won / (Renew.Won + Renew.Lost),2),
    still.open =         length(Warboard_Category__c[which(open == T)]),
    open.usd =           sum(Qualified_Bookigns[which(open == T)],na.rm = T),
    forecast.usd =       sum(forecast,na.rm = T),
    forecast.net.pim =   forecast.usd / Renew.avail.usd,
    standard.dollar.retention = sum(sdr.numerator,na.rm = T) / sum(Recurring_Booking_PY,na.rm = T),
    Projected.price.increase.capture = sum(forecast,na.rm = T) / sum(Recurring_Booking_PY,na.rm = T)
  )

cam.scorecard.sao <- opps %>%
  filter(SAO_Date__c >= p.start & # here is the place to change the date range
           SAO_Date__c < p.end &
           !(Warboard_Category__c %in% renewals) & 
           !is.na(SAO_Date__c)
) %>%
  group_by(Owner_Name)%>%
  summarise(
    sao.num =       length(Warboard_Category__c),
    pipe.created =  sum(Qualified_Bookigns, na.rm = T)
  )

cam.scorecard <- merge(cam.scorecard.renewals,cam.scorecard.sao,
                       by = 'Owner_Name',
                       all.x = TRUE)

# replace inf and NaN with NA
for (c in names(cam.scorecard)) {
  cam.scorecard[which(is.na(cam.scorecard[,c])),c] <- NA
  cam.scorecard[which(cam.scorecard[,c] == "Inf"),c] <- NA
}

cam.col.order <- c(
  "Owner_Name",
  "Renew.avail.Num",
  "Renew.avail.usd",
  "Renew.Won.QB.usd",
  "gross.pim",
  "retention",
  "forecast.net.pim",
  "still.open",
  "open.usd",
  "sao.num",
  "pipe.created",
  "Renew.Won",
  "Renew.Lost",
  "Renew.Won.RBPY.usd",
  "forecast.usd",
  "standard.dollar.retention",
  "Projected.price.increase.capture"
)

if(!all(names(cam.scorecard) %in% cam.col.order)){
  stop("check your cam cols")
}
cam.scorecard <- cam.scorecard[,cam.col.order]
names(cam.scorecard)[1] <- "Owner Name"

write_sheet(cam.scorecard,sheet.link,
            sheet = paste0('cam.scorecard.',p))

}

# table for deals that are still in play
pipe.metrics <- opps %>% 
  filter(rpt_qtr %in% c(tq) & # here is where to change the date
           Qualified_Bookigns > 0 &
           !(Warboard_Category__c %in% c("Professional Services","Data Error - See Rev Ops")) &
           # remove early stages
           !(Stage %in% c('Closed Lost',
                          "Discovery",
                          "Data Quality",
                          "Nurture",
                          "Temporary",
                          "Disqualified",
                          "Identify",
                          "Attempting Contact",
                          # "Demonstration", # removed demo from exception list to include 
                          "Untouched"))) %>%
  # create stage as a factor variable so it sorts in the correct order
  mutate(`Stage Name` = factor(Stage, levels = c("Engaged",
                                                 "Demonstration",
                                                 "Business Review",
                                                 "Offer",
                                                 "Sales Accepted Opp",
                                                 "Solution Overview",
                                                 "Eval Planning",
                                                 "POC",
                                                 "Tech Eval",
                                                 "Proposal",
                                                 "Negotiation",
                                                 "Awaiting Approval",
                                                 "Sign-Off",
                                                 "Order Confirmed",
                                                 "Closed Won"), ordered = TRUE)) %>%
  group_by(Owner_Name,`Stage Name`) %>%
  summarise(Value = sum(Qualified_Bookigns, na.rm = T),
            Count = length(Opp_Name),
            `Days In Stage` = paste(Days_Stale,collapse = ", ")) %>%
  group_by(Owner_Name) %>%
  mutate(
    `% of Total` = Value / sum(Value),
    is.renewal = `Stage Name` %in% c("Engaged",
                                     "Business Review",
                                     "Offer",
                                     "Awaiting Approval",
                                     "Order Confirmed")) %>%
  arrange(Owner_Name,desc(`Stage Name`))

names(pipe.metrics)[1] <- "Owner Name"

share <- opps %>% 
  filter(rpt_qtr %in% c(tq) & # here is where to change the date
           Qualified_Bookigns > 0 &
           !(Warboard_Category__c %in% c("Professional Services","Data Error - See Rev Ops")) &
           !(Stage %in% c(closed.stages,
                          "Discovery",
                          "Data Quality",
                          "Nurture",
                          "Temporary",
                          "Disqualified",
                          "Attempting Contact",
                          "Demonstration",
                          "Untouched",
                          "Identify"))) %>%
  group_by(Owner_Name) %>%
  arrange(Qualified_Bookigns) %>%
  slice_tail(n = 10) %>%
  arrange(Owner_Name,desc(Qualified_Bookigns))

share2 <- share[,c(
                  "Owner_Name",
                  "rpt_qtr",
                  "Opp_Name_link",
                  "Stage",
                  "Warboard_Category__c",
                  "Qualified_Bookigns",
                  # "Partner Account", # nix the spice columns
                  "Close_Date",
                  "MEDDPICC_Score__c",
                  "Days_Stale"
                  # "Account_Name",
                  # "Situation",
                  # "Pain",
                  # "Impact",
                  # "Critical/Compelling Event",
                  # "Decision Making Process & Authority"
                  )]
# share2 <- share2[order(share2$`Opportunity Owner`,
#                      share2$rpt_qtr,
#                      share2$`Qualified Bookings (converted)`
#                      ),]

names(share2) <- c("Opportunity Owner",
                   "Quarter",
                   "Opportunity Name",
                   "Stage",
                   "Type",
                   "Amount",
                   "Close Date",
                   "MEDDPICC Score",
                   "Days in Stage")

share2$Quarter[which(share2$Quarter == tq)] <- tq.format
share2$Quarter[which(share2$Quarter == nq)] <- nq.format

# share2$Type[which(grepl(share)]

# shorten the types for easy
# share2$

# share2$`Qualified Bookings` <- format.money(share2$`Qualified Bookings`)

# write_sheet(share,"https://docs.google.com/spreadsheets/d/1ouTj1K3UGfgzmwxZWfsZX6nEJfbkWxU9rxa8Va9h9-k/edit#gid=0",
#             sheet = 'Share')

write_sheet(share2,sheet.link,
            sheet = 'Share2')

write_sheet(pipe.metrics,sheet.link,
            sheet = 'pipe.metrics')

write_sheet(opps,sheet.link,
            sheet = 'raw data')

activities <- query.bq(
  paste0(
"
select Name,Metric,
sum(Amount) as Amount from `Outreach.Outreach_Metric_Table`
where ActivityDate >= '",unit.start,"'
and   ActivityDate <  '",unit.end,"'
group by Name, Metric
order by Name, Metric
    "
  )
)

CAM <- c(
  "Isaiah Caba",
  "Loren Staselewicz",
  "Michael Ries",
  "Natalie Faria Buckner"
)
SAM <- c(
  "Jeff Kobryn",
  "Ron Whitling",
  "Jeremy Sessoms"
)
EMEA.AE <- c(
  "Mark Robinson",
  "James Melville",
  "Jakub Witczak",
  "Neil Hamilton",
  "Ben Geleit"
)
Ent.AE <- c(
  "Owen Bailey",
  "Simon Reynolds",
  "Mark Fackler",
  "Kevin Yost",
  "Matthew O\'Haugherty"
)
com.AE <- c(
  "Patrick Knorring",
  "Kendal Powell",
  "Josh Ury",
  "Wes Jensen",
  "Oliver Eden"
)

all.qbr.reps <- unique(c(CAM,SAM,EMEA.AE,Ent.AE,com.AE))

activities.wide <- activities %>%
  pivot_wider(names_from = Metric, values_from = Amount)

activities.wide$group <- NA
activities.wide$group[which(activities.wide$Name %in% CAM)] <- 'CAM'
activities.wide$group[which(activities.wide$Name %in% SAM)] <- 'SAM'
activities.wide$group[which(activities.wide$Name %in% EMEA.AE)] <- 'EMEA'
activities.wide$group[which(activities.wide$Name %in% Ent.AE)] <- 'Enterprise NA'
activities.wide$group[which(activities.wide$Name %in% com.AE)] <- 'Commercial NA'

write_sheet_keep_sheet_format(
  activities.wide,
  sheet.link,
  sheet.name = 'Rep Activities'
)

# make a table to include all reps
all.reps <- opps %>%
  group_by(User_Role,Owner_Name) %>%
  filter(Cohort_Close_Date >= unit.start &
           Cohort_Close_Date < unit.end &
           !is.na(SAO_Date__c)) %>%
  summarise(Wins = sum(Stage == 'Closed Won'),
            Losses = sum(Stage == 'Closed Lost'),
            Win.Rate = Wins/(Losses+Wins),
            Won.USD = sum(Qualified_Bookigns[which(Stage == 'Closed Won')],na.rm = T)
            )
# 
write_sheet(all.reps,sheet.link,
            sheet = 'all.reps')


time.in.stage <- query.bq(
  paste0(
"
WITH opps AS (
  SELECT
    u.Manager__c AS Opp_Owner_Manager,
    o.Id AS Opp_Id,
    o.Name AS Opp_Name,
    ARR_Product__c AS Product,
    ACV_Bookings__c / ct.ConversionRate AS QB_USD,
    u.Name AS Opp_Owner,
    o.StageName,
    o.Warboard_Category__c,
    o.Type,
    date_diff(SAO_Date__c, Discovery_Set_Date__c, DAY) AS Discovery,
    date_diff(Eval_Planning_Date__c, SAO_Date__c, DAY) AS SAO,
    date_diff(Tech_Eval_Date__c, Eval_Planning_Date__c, DAY) AS Eval_Planning,
    date_diff(Negotiation_Date__c, Tech_Eval_Date__c, DAY) AS Tech_Eval,
    date_diff(CloseDate, Negotiation_Date__c, DAY) AS Negotiation,
    date_diff(CloseDate, SAO_Date__c, DAY) AS SAO_to_Close
  FROM `skyvia.Opportunity` o
  LEFT JOIN skyvia.User u ON o.OwnerId = u.Id
  LEFT JOIN skyvia.CurrencyType ct ON o.CurrencyIsoCode = ct.IsoCode
  WHERE Test_Account__c = FALSE
    AND StageName = 'Closed Won'
    -- AND StageName not in ('Temporary','Data Quality')
    AND Warboard_Category__c in ('New','Expansion','SAP - Expansion')
    AND CloseDate >= '2022-07-01'
)
SELECT
  Opp_Owner_Manager,
  Opp_Id,
  Opp_Name,
  Product,
  QB_USD,
  Opp_Owner,
  StageName,
  Warboard_Category__c,
  Type,
  'Discovery' AS Stage,
  Discovery AS Days
FROM opps
UNION ALL
SELECT
  Opp_Owner_Manager,
  Opp_Id,
  Opp_Name,
  Product,
  QB_USD,
  Opp_Owner,
  StageName,
  Warboard_Category__c,
  Type,
  'Solution Overview' AS Stage,
  SAO AS Days
FROM opps
UNION ALL
SELECT
  Opp_Owner_Manager,
  Opp_Id,
  Opp_Name,
  Product,
  QB_USD,
  Opp_Owner,
  StageName,
  Warboard_Category__c,
  Type,
  'Eval Planning' AS Stage,
  Eval_Planning AS Days
FROM opps
UNION ALL
SELECT
  Opp_Owner_Manager,
  Opp_Id,
  Opp_Name,
  Product,
  QB_USD,
  Opp_Owner,
  StageName,
  Warboard_Category__c,
  Type,
  'Tech Eval' AS Stage,
  Tech_Eval AS Days
FROM opps
UNION ALL
SELECT
  Opp_Owner_Manager,
  Opp_Id,
  Opp_Name,
  Product,
  QB_USD,
  Opp_Owner,
  StageName,
  Warboard_Category__c,
  Type,
  'Negotiation' AS Stage,
  Negotiation AS Days
FROM opps
"
  ))

stage.order <- c(
  "Discovery",
  "Solution Overview",
  "Eval Planning",
  "Tech Eval",
  "Negotiation"
)

time.in.stage$Stage <- factor(time.in.stage$Stage,levels = stage.order)

fill.bads <- function(x){
  x[which(is.nan(x))] <- NA
  x[which(x<0)] <- 0
  return(x)
}

managers.closed.won <- 
  time.in.stage %>%
  filter(StageName == 'Closed Won' & !is.na(Days)) %>%
  group_by(Opp_Owner_Manager,Stage, .drop = FALSE) %>%
  summarise(mean_days = fill.bads(round(mean(Days,na.rm = T))),
            median_days = fill.bads(round(median(Days,na.rm = T))),
            # n = length(Days)
            ) %>%
  mutate(
    helper = paste0(Opp_Owner_Manager,Stage),
    share  = paste0(mean_days,", ",median_days)
  )

open.pipe <- query.bq(
  "
select
    u.Manager__c Opp_Owner_Manager,
    u.Name Opp_Owner,
    o.StageName Stage,
    o.Name Opp_Name,
    o.Days_Stale__c
from skyvia.Opportunity o
left join skyvia.User u on o.OwnerId = u.Id
where StageName in (
'Discovery',
'Solution Overview',
'Eval Planning',
'Tech Eval',
'Negotiation'
    )
and Test_Account__c = FALSE
and Warboard_Category__c in ('New','Expansion','SAP - Expansion')
  "
)

open.pipe$Stage <- factor(open.pipe$Stage,levels = stage.order)

my.open.pipe <- 
  open.pipe %>%
  filter(Opp_Owner %in% all.qbr.reps) %>%
  group_by(Opp_Owner_Manager,Opp_Owner,Stage, .drop = FALSE) %>%
  summarise(mean_days = fill.bads(round(mean(Days_Stale__c,na.rm = T))),
            median_days = fill.bads(round(median(Days_Stale__c,na.rm = T)))
            # n = length(Opp_Name)
            ) %>%
  mutate(
    helper = paste0(Opp_Owner_Manager,Stage),
    share  = paste0(mean_days,", ",median_days)
  )

names(managers.closed.won)[which(names(managers.closed.won) != 'helper')] <- paste0(
"Team_",names(managers.closed.won)[which(names(managers.closed.won) != 'helper')]
)

share.pipe <- merge(my.open.pipe,managers.closed.won[,c("Team_share","Team_mean_days","Team_median_days","helper")],by = 'helper')
share.pipe <- share.pipe %>%
  arrange(Opp_Owner,Stage)

share.pipe.upload <- share.pipe[,c("Opp_Owner_Manager","Opp_Owner","Stage","share","Team_share")]
names(share.pipe.upload) <- gsub("share","mean_median",names(share.pipe.upload))
share.pipe.upload$mean_median[which(grepl('NA',share.pipe.upload$mean_median))] <- "."
share.pipe.upload$Team_mean_median[which(grepl('NA',share.pipe.upload$Team_mean_median))] <- "."
write_sheet(share.pipe.upload,
            sheet.link,
            'Deal Velocity')

# paste.excel(opps[which(opps$Owner_Name == 'Neil Hamilton' & opps$rpt_qtr == "2023.2"),])

# 
# scorecards <- opps %>%
#   filter(rpt_qtr == pq) %>%
#   group_by(`Opportunity Owner`) %>%
#   summarise(ActualBookings = sum(`Qualified Bookings (converted)`,na.rm = T))
# 
# 
# scorecards.raw <- opps %>%
#   filter(rpt_qtr == pq) %>%
#   group_by(OppOwnerName)