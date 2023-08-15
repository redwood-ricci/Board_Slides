library(bigrquery)
library(salesforcer)
library(dplyr)
library(googlesheets4)
library(stringr)
library(googledrive)
library(lubridate)

# this is to authorize google to work on the files
# drive_auth()
# gs4_auth(token = drive_token())

# The purpose of this function is to take a data frame and make it into a convienent format
# for pasting into a spreadsheet. The function takes a data frame as input and
# copies it as tab delim directly to clipboard. Run the function and Ctrl + V into text
paste.excel <- function(x,row.names=FALSE,col.names=TRUE,...) {
  write.table(x,"clipboard",sep="\t",row.names=row.names,col.names=col.names,...)
}

#format method, which is necessary for formating in a data.frame   
format.money  <- function(x, digits = 2) {
  paste0("$", formatC(as.numeric(x), format="f", digits=digits, big.mark=","))
}

#method for writing into google sheet and preserving formating
write_sheet_keep_sheet_format <- function(data,link.to.sheet,sheet.name){
# the purpose of this function is to write data into a goodle sheet and preserve the formating in that sheet
# it writes a load of blank data into a existing sheet then writes the new data frame
# it takes a data frame, a google sheet link, and the name of the sheet tab as arguments
# then outputs written into the google sheet
  
  range_flood(link.to.sheet,
              sheet = sheet.name,
              reformat = FALSE)
  range_write(link.to.sheet,
              data,
              sheet = sheet.name,
              reformat = FALSE)
}

# Plotly Keys
Sys.setenv("plotly_username"="mricci_redwood")
Sys.setenv("plotly_api_key"="LL5kw4B7Y7nEDFGwOQrg")

clean.names <- function(df.names){
  # this function cleans the names of a data frame to uploading to bigquery
  # it takes a list of names as an argument
  # and returns a list of names that is safe for column names in bigquery
  df.names <- gsub("\\.","_",df.names)
  df.names <- gsub(" ","_",df.names)
  df.names <- gsub("'","",df.names)
  df.names <- str_replace_all(df.names, "[^[:alnum:]_]", "")
  return(df.names)
}


get.sf.object <- function(object='list',instance='Redwood', clean.names=TRUE){
  # the purpose of this function is to download a table from salesforce
  # it takes a salesforce table name as an argument
  # and returns a data frame containing the table requested
  # this function also converts currency fields to USD # see below
  # if run blank this function returns a list of the available objects from a particular instnace
  # default instance is Redwood
  
  # test object
  # object <- "PricebookEntry"
  
  print("Authorizing")
  if(instance == 'Redwood'){
    sf_auth(cache = ".httr-oauth-salesforcer-redwood")
    
  }else if(instance == 'ASCI'){
    sf_auth(cache = ".httr-oauth-salesforcer-asci")  
    
  }else{
    stop('Please pick instance ASCI or Redwood')
  }
  
  if(object == 'list'){
    soql.all.objects <- paste0("SELECT  QualifiedApiName FROM EntityDefinition order by QualifiedApiName")
    all.sf.ojbects <- sf_query(soql.all.objects) # execute query
    print(paste0('Returned ',instance,"_fields"))
    return(all.sf.ojbects)
  }

  print("Listing Fields")
  fields <- sf_describe_object_fields(object)
  
  # convert currency fields
  if(any(fields$type == 'currency')){ # convert currency fields where applicable
    print("Converting Currency")
    currency.fields <- paste0("convertCurrency(",fields$name[which(fields$type == 'currency')],") ",
                              fields$name[which(fields$type == 'currency')],"_Converted")
    
    fields.string <- paste0(c(fields$name,currency.fields),collapse = ",")
  }else{
    fields.string <- paste0(fields$name,collapse = ",")
  }
  
  soql <- paste0("SELECT ",fields.string," FROM ",object) # form query for all opp fields and rows
  
  return_name <- paste0(instance,"_",object)
  
  print(paste0("Running: ",return_name))
  
  download <- sf_query(soql)
  if(clean.names){
    names(download) <- clean.names(names(download))
  }
  
  # assign(return_name,download,envir = globalenv()) # execute query
  print(return_name)
  return(download)
  # return(paste0("Returned: ",return_name))
  
}

# authorization for bigquery
bq_auth(path = "activeco-4ac3f78ba057.json")
# gs4_auth(path = "drive-activeco-d876f1f4439f.json") # service account needs direct access to each sheet. How to share entire drive with service account?

upload.to.bigquery <- function(upload,dataset,table){
  print(paste0("Uploading to Bigquery: ",dataset,".",table))
  data.set.bq <- bq_dataset(billing,dataset) # create redwood dataset
  table.bq <- bq_table(data.set.bq, table)
  bq_perform_upload(table.bq,fields = upload, values = upload,
                    create_disposition = "CREATE_IF_NEEDED",
                    write_disposition = "WRITE_TRUNCATE")
  
}

sf.table.to.bigquery <- function(sf.table,instance = 'Redwood'){
  # the purpose of this function is to download a table from salesforce and upload it to bigquery
  # it takes a salesforce table name and a SF instance as an argument and uploads that table to bigquery
  sf.download <- get.sf.object(sf.table,
                               instance = instance)
  
  upload.to.bigquery(sf.download,instance,sf.table)

}

df.lists.to.string <- function(df){
  # the purpose of this function it to take one of the objects returned by the function sf_describe_object_fields() from
  # salesforcer and return a data frame with no columns that contain lists
  # this lets the resulting data frame be uploaded to google sheets without errors
  list.vars <- 
    names(
      df %>%  
        select_if(is.list)
    )
  
  
  for(n in list.vars){
    newcol <- paste0("_",n)
    df[,newcol] <- NA
    for(i in 1:nrow(df)){
      df[i,newcol] <- paste0(df[i,n])
    }
    df[,n] <- NULL
  }
  
  df %>%
    relocate(label, .before = everything()) %>% # relocate col names for easier reading later
    relocate(name, .before = everything()) %>% # relocate col names for easier reading later
    return(df)
}

# sf.table.to.bigquery("User")
query.bq <- function(query,page.size = NULL){
  # the purpose of this function is to take SQL and run it agians the bigquery
  # instance and return a data frame
  # it takes SQL as an input and returns a data frame from the activeco project
  # containging the querried data
billing <-  "activeco"
  
  tab <- bq_project_query(billing,query)
  
  df <- bq_table_download(tab, page_size = page.size)

  return(df)
}


Redwood.opp.links <- function(opp.id){
  # the purpose of this function is to take opportunity ID numbers and make them into links to the redwood salesforce instance
  # it takes an array of ID numbers as an input and returns an aray of links
  return(paste0('https://ribg.lightning.force.com/lightning/r/Opportunity/',opp.id,'/view'))
  
}

ASCI.opp.links <- function(opp.id){
  # the purpose of this function is to take opportunity ID numbers and make them into links to the ASCI salesforce instance
  # it takes an array of ID numbers as an input and returns an aray of links
  print("ASCI.opp.links depricated, use ASCI.links instead")
  return(paste0('https://oneredwood.lightning.force.com/lightning/r/Opportunity/',opp.id,'/view'))

}

ASCI.links <- function(opp.id){
  # the purpose of this function is to take opportunity ID numbers and make them into links to the ASCI salesforce instance
  # it takes an array of ID numbers as an input and returns an aray of links
  return(paste0('https://oneredwood.lightning.force.com/lightning/r/Opportunity/',opp.id,'/view'))
}

Redwood.act.links <- function(id){
  # the purpose of this function is to take Account ID numbers and make them into links to the redwood salesforce instance
  # it takes an array of ID numbers as an input and returns an aray of links
  return(paste0('https://ribg.lightning.force.com/lightning/r/Account/',id,'/view'))
  
}

ASCI.act.links <- function(id){
  # the purpose of this function is to take Account ID numbers and make them into links to the ASCI salesforce instance
  # it takes an array of ID numbers as an input and returns an aray of links
  return(paste0('https://advsyscon.lightning.force.com/lightning/r/Account/',id,'/view'))
  
}

redwood.products <- c(
  "Insight",
  "SAP BPA",
  "Unknown",
  "Cronacle",
  "RunMyJobs",
  "Report2Web",
  "Missing Value",
  "RMJ On Premise",
  "RunMyJobs SaaS",
  "Active Monitoring",
  "SAP BPA;RunMyJobs",
  "Finance Automation",
  "Services - SAP BPA",
  "Services Report2Web",
  "Services Scheduling",
  "Report2Web;RunMyJobs",
  "RunMyJobs On Premise",
  "Finance Automation SaaS",
  "Cronacle;Active Monitoring",
  "SAP BPA;Finance Automation",
  "SAP BPA;Services - SAP BPA",
  "Services Finance Automation",
  "Services - SAP BPA;RunMyJobs",
  "Finance Automation On Premise",
  "RunMyJobs SaaS;RMJ On Premise",
  "Services Scheduling;RunMyJobs",
  "Active Monitoring;Active Auditing",
  "RunMyJobs SaaS;RunMyJobs On Premise",
  "Services - SAP BPA;Finance Automation",
  "RunMyJobs SaaS;Finance Automation SaaS",
  "Services Scheduling;Services - SAP BPA",
  "Cronacle;Active Monitoring;Active Auditing",
  "Services Finance Automation;Finance Automation",
  "Report2Web;Services Report2Web",
  "Services Scheduling;RunMyJobs;Finance Automation",
  "SAP BPA;Cronacle;Active Monitoring",
  "Report2Web;SAP BPA",
  "SAP BPA;Services Scheduling;RunMyJobs",
  "RunMyJobs SaaS;SAP BPA",
  "Report2Web;Finance Automation",
  "Report2Web;SAP BPA;RunMyJobs",
  "RunMyJobs;Finance Automation",
  "Report2Web;RunMyJobs;Finance Automation",
  "Services Report2Web;Services - SAP BPA",
  "Report2Web;Services - SAP BPA;RunMyJobs",
  "Services Scheduling;Services Finance Automation;Services Report2Web;Services - SAP BPA",
  "Services Scheduling;Services Report2Web",
  "Services Scheduling;Finance Automation",
  "Services Scheduling;Services - SAP BPA;RunMyJobs",
  "Active Auditing",
  "SAP BPA;Services Scheduling;Services - SAP BPA;RunMyJobs",
  "SAP BPA;Cronacle",
  "SAP BPA;Services - SAP BPA;RunMyJobs",
  "SAP BPA;Services Scheduling",
  "SAP BPA;RunMyJobs;Finance Automation",
  "Redwood Robotics"
)

asci.products <- c(
  "JSCAPE",
  "ActiveBatch",
  "XLNT",
  "Virtuoso",
  "MFT",
  "RemoteSHADOW",
  "INTACT "
)

billing.test.api <- "OG3q0Bi5ktflR0OCyinInrP9pRc2MJOFsPDx2tIj6lTNN20c7pNFJOhUalZj4Uk9"

string.in.for.query <- function(string.list){
  # the purpose of this function is to take a vector of strings and return them in the format
  # so they can be used in an SQL query. It takes a vector and returns one string for a query
  paste0("'",string.list,collapse = ",","'")
}

# define grouping variables
  # flip types
flip.types <- c(
  "Grandfathered Renewal",
  "Subscription",
  "Subscription Flip",
  "Term Flip",
  "SAP Flip"
)
term.flip.types <- c(
  "Term Flip"
)

maintenance.flips <- c(
  "Subscription Flip"
)

sap.flip.types <- c(
  "SAP Flip"
)
  
new.business.types <- c(
  "New Logo",
  "New Business",
  "Pilot / POC"
)

expansion.types <- c(
  "Existing Customer",
  "Add-on Sales",
  "Existing Business",
  "On Premise to SaaS",
  "Perpetual to Term"
)

professional.services.types <- c(
  "Professional Services"
)

junk.types <- c(
  "null",
  "Temporary",
  "Expenses"
)

renewal.types <- c(
  "Renewal",
  "Renewal Business"
)

types <- c(
  flip.types,
  term.flip.types,
  maintenance.flips,
  sap.flip.types,
  new.business.types,
  expansion.types,
  professional.services.types,
  junk.types,
  renewal.types
)

closed.won.stages <- c("5-Closed","Closed Won")
closed.lost.stages <- c("99-Lost","Closed Lost")
closed.stages <- c(closed.won.stages,closed.lost.stages)
junk.stages <- c(
  'Unqualified', #
  'Disqualified', #
  'Needs Analysis',
  '0-Suspect',
  'Attempting Contact',
  'Discovery', #
  'Temporary', #
  'Untouched', #
  'No Response',
  'Closed Deferred',
  'Identified', #
  'Alternate Configuration',
  'Data Quality', #
  'Nurture' #
)

# this is for Bigquery. It's the project billing name to be used in any bigqueryr calls
billing <-  "activeco"

all.regions <- c(
  "Benelux",
  "North America",
  "DACH",
  "UK",
  "APJ",
  "Rest of EMEA",
  "LATAM",
  "Nordics",
  "Other",
  "Eastern Europe")

emea.regions <- c(
  "Nordics",
  "Other",
  "Eastern Europe",
  "Rest of EMEA",
  "DACH",
  "UK",
  "APJ",
  "Benelux"
)

north.america.regions <- c(
  "North America",
  "LATAM"
)

all.products <- c(
  "RunMyJobs",
  "Services Report2Web",
  "Report2Web",
  "Services Scheduling",
  "Services - SAP BPA",
  "Cronacle",
  "SAP BPA",
  "SAP BPA;RunMyJobs",
  "Services Scheduling;RunMyJobs",
  "SAP BPA;Finance Automation",
  "RunMyJobs SaaS",
  "Finance Automation",
  "Services Finance Automation",
  "Services - SAP BPA;RunMyJobs",
  "Finance Automation On Premise",
  "Finance Automation SaaS",
  "SAP BPA;Services - SAP BPA",
  NA,
  "Report2Web;Services Report2Web",
  "RunMyJobs On Premise",
  "Services Scheduling;RunMyJobs;Finance Automation",
  "SAP BPA;Cronacle;Active Monitoring",
  "Active Monitoring;Active Auditing;Active Archiving",
  "Report2Web;SAP BPA",
  "SAP BPA;Services Scheduling;RunMyJobs",
  "Active Monitoring",
  "RunMyJobs SaaS;SAP BPA",
  "Report2Web;Finance Automation",
  "Report2Web;RunMyJobs",
  "RunMyJobs;Finance Automation",
  "Report2Web;RunMyJobs;Finance Automation",
  "Services Report2Web;Services - SAP BPA",
  "Report2Web;Services - SAP BPA;RunMyJobs",
  "Services Scheduling;Services Finance Automation;Services Report2Web;Services - SAP BPA",
  "Services Scheduling;Services - SAP BPA",
  "RMJ On Premise",
  "Insight",
  "Services Scheduling;Services Report2Web",
  "Services - SAP BPA;Finance Automation",
  "Services Scheduling;Finance Automation",
  "Services Scheduling;Services - SAP BPA;RunMyJobs",
  "Active Auditing",
  "SAP BPA;Services Scheduling;Services - SAP BPA;RunMyJobs",
  "Active Monitoring;Active Auditing",
  "SAP BPA;Cronacle",
  "SAP BPA;Services - SAP BPA;RunMyJobs",
  "Cronacle;Active Monitoring",
  "SAP BPA;Services Scheduling",
  "SAP BPA;RunMyJobs;Finance Automation",
  "Redwood Robotics",
  "Cronacle;Active Monitoring;Active Auditing",
  "RunMyJobs SaaS;RunMyJobs On Premise",
  "Unknown",
  "RunMyJobs SaaS;Finance Automation SaaS",
  "Missing Value",
  "RunMyJobs SaaS;RMJ On Premise",
  "SAP BPA;Services - SAP BPA;Finance Automation",
  "SLA",
  "Services Finance Automation;Finance Automation",
  "ActiveBatch",
  "JSCAPE",
  "XLNT",
  "Services",
  "RemoteSHADOW",
  "Virtuoso",
  "MFT",
  "INTACT"
)

rmj.products <- c(
  "RunMyJobs",
  "Cronacle",
  "RunMyJobs SaaS",
  "RunMyJobs On Premise",
  "RunMyJobs SaaS;SAP BPA",
  "Report2Web;RunMyJobs",
  "RMJ On Premise",
  "Cronacle;Active Monitoring",
  "Cronacle;Active Monitoring;Active Auditing",
  "RunMyJobs SaaS;RunMyJobs On Premise",
  "RunMyJobs SaaS;RMJ On Premise"
)

fa.products <- c(
  "SAP BPA;Finance Automation",
  "Services Scheduling;RunMyJobs;Finance Automation",
  "Finance Automation",
  "Finance Automation On Premise",
  "Finance Automation SaaS",
  "RunMyJobs SaaS;Finance Automation SaaS",
  "Services Scheduling;RunMyJobs;Finance Automation",
  "Report2Web;Finance Automation",
  "RunMyJobs;Finance Automation",
  "Report2Web;RunMyJobs;Finance Automation",
  "Services - SAP BPA;Finance Automation",
  "Services Scheduling;Finance Automation",
  "SAP BPA;RunMyJobs;Finance Automation",
  "RunMyJobs SaaS;Finance Automation SaaS",
  "SAP BPA;Services - SAP BPA;Finance Automation",
  "Services Finance Automation;Finance Automation",
  "Redwood Robotics",
  "RunMyJobs;Finance Automation",
  "Report2Web;RunMyJobs;Finance Automation"
)

mft.products <- c(
  "JSCAPE",
  "MFT"
)

ab.products <- c(
  "ActiveBatch"
)

asci.other <- c(
  "XLNT",
  "INTACT",
  "Virtuoso",
  "RemoteSHADOW"
)

asci.products <- c(mft.products,ab.products,asci.other)

redwood.products <- all.products[!(all.products %in% asci.products)]

all.funnels <- c(
  "Expansion",
  "New Sales - Outbound",
  "New Sales - Inbound"
)

scrub_usd <- function(salesforce_usd){
  # the purpose of this function is to remove formatting from a salesforce report currency field
  # E.g., "USD 2,000.00" to "2000.0" so it can be read by numeric functions
  # it takes a vecor as an input and outputs a numeric vector
  
  out <- gsub("USD ","",salesforce_usd)
  out <- gsub(",","",out)
  out <- gsub("-",NA,out)
  out <- as.numeric(out)
  return(out)
}

scrub_currency <- function(salesforce_usd){
    # this function is replacting scrub_usd and works with all SF currencies instead of just USD
    # the purpose of this function is to remove formatting from a salesforce report currency field
    # E.g., "USD 2,000.00" to "2000.0" so it can be read by numeric functions
    # it takes a vecor as an input and outputs a numeric vector
  
    currencies <- query.bq('select IsoCode from `activeco.skyvia.CurrencyType`')
    for ( c in 1:nrow(currencies)) {
        salesforce_usd <- gsub(paste0(as.character(currencies[c,1])," "),"",salesforce_usd)
    }
    out <- gsub(",","",salesforce_usd)
    out <- gsub("-",NA,out)
    out <- as.numeric(out)
    return(out)
  }

asci.hyperlinks <- function(id,name){
  # the purpose of this function is to take a list of ids and text names and convert them
  # into hyperlinks inside a google sheet. For example it takes an opportunity ID and an
  # opportunity name and returns a link to that opportunity in salesforce
  links <- gs4_formula(paste0("=HYPERLINK(\"",ASCI.links(id)
                     ,"\",","\"",name,"\")"))
  return(links)
}


make.sfdc.update <- function(file.header = NA,og.upload = NA, upload = NA,object.to.edit = NA,notes = NA){
  
  # the purpose of this function is to upload data to SFDC
  # and write an audit file to google drive so you can look back later to see uploads
  # this function can only update existing records
  
  # file.header should be the text name of the file you want to be uploaded
  # og.upload should be the original version of the data you want to upload for reference if you want to revert back later
  # the upload is the data that will changed in SFDC
  # and the object to edit is the SFDC object you want to edit
  # notes get writen into the sheet as text for a reminder later why this upload was made
  
  # Program used to resize columns in upload, takes way too long, easier to do in google UI
  # code for resizing is commented out below
  
  
  file.name <- paste0(Sys.time()," ",file.header)
  
  # create a place to put the upload results
  print("Creating Upload Spreadsheet")
  upload.location <- drive_create(name = file.name,
                                  path = "https://drive.google.com/drive/folders/12U5Al388tw4YpBU3R3kfb_pBxIBRCa7P", # link to Bulk R folder
                                  type ="spreadsheet",
                                  overwrite = FALSE # should only be new files
                                  )
  
  # get the link to the new upload file
  upload.link <- upload.location$drive_resource[[1]]$webViewLink
  
  # rename 'Sheet1'
  sheet_rename(upload.link,sheet = 'Sheet1',new_name = 'Before Upload')
  
  # write the before and after files
  print("Writing OG File")
  write_sheet(og.upload,
              upload.link,
              sheet = 'Before Upload')
  
  # print("OG File Col Resize")
  # range_autofit(upload.link,sheet = 'Before Upload')
  
  print("Writing Upload File")
  write_sheet(upload,
              upload.link,
              sheet = 'Upload File')
  
  # print("Upload File Col Resize")
  # range_autofit(upload.link,sheet = 'Upload File')
  
  # Upload the records to Salesforce
  print("Uploading Records to Salesforce")
  updated_records <- sf_update(upload,
                               object_name=object.to.edit)
  
  # convert error list to string if there are errors
  if(any(grepl('errors',names(updated_records)))){
    print("Writing Error Log")
    updated_records$errors <- as.character(updated_records$errors)
  }
  
  updated_records$id <- upload$id
  
  # write the results
  print("Uploading Results")
  write_sheet(updated_records,
              upload.link,
              sheet = 'Upload Results')
  # print("Resizing Upload Result Cols")
  # range_autofit(upload.link,sheet = 'Upload Results')
  if(nchar(notes)>=50000){
    warning("notes longer than 50k, truncating")
    notes <- strtrim(notes,49999)
  }
  # upload notes if there are any
  if(!is.na(notes)){
    print("Writing Notes")
    write_sheet(data.frame(notes = notes,
                           row.names = FALSE),
                upload.link,
                sheet = 'Notes')
    range_autofit(upload.link,sheet = 'Notes')
  }
  print(paste0("Done, safe to check file @ ",upload.link))
}

make.sfdc.upsert <- function(file.header = NA, og.upload = NA, upload = NA,
                             external.id = NA, object.to.edit = NA, notes = NA){
  
  # the purpose of this function is to upload/insert data to SFDC
  # its very similar to the make.sfdc.update function except this function can also create new records
  # and write an audit file to google drive so you can look back later to see uploads
  # this function can only update existing records
  
  # file.header should be the text name of the file you want to be uploaded
  # og.upload should be the original version of the data you want to upload for reference if you want to revert back later
  # the upload is the data that will changed in SFDC
  # and the object to edit is the SFDC object you want to edit
  # notes get writen into the sheet as text for a reminder later why this upload was made
  
  # Program used to resize columns in upload, takes way too long, easier to do in google UI
  # code for resizing is commented out below
  
  
  file.name <- paste0(Sys.time()," ",file.header)
  
  # create a place to put the upload results
  print("Creating Upload Spreadsheet")
  upload.location <- drive_create(name = file.name,
                                  path = "https://drive.google.com/drive/folders/12U5Al388tw4YpBU3R3kfb_pBxIBRCa7P", # link to Bulk R folder
                                  type ="spreadsheet",
                                  overwrite = FALSE # should only be new files
  )
  
  # get the link to the new upload file
  upload.link <- upload.location$drive_resource[[1]]$webViewLink
  
  # rename 'Sheet1'
  sheet_rename(upload.link,sheet = 'Sheet1',new_name = 'Before Upload')
  
  # write the before and after files
  print("Writing OG File")
  write_sheet(og.upload,
              upload.link,
              sheet = 'Before Upload')
  
  # print("OG File Col Resize")
  # range_autofit(upload.link,sheet = 'Before Upload')
  
  print("Writing Upload File")
  write_sheet(upload,
              upload.link,
              sheet = 'Upload File')
  
  # print("Upload File Col Resize")
  # range_autofit(upload.link,sheet = 'Upload File')
  
  # Upload the records to Salesforce
  print("Uploading Records to Salesforce")
  updated_records <- sf_upsert(upload,
                               object_name=object.to.edit,
                               external_id_fieldname = external.id)
  
  # convert error list to string if there are errors
  if(any(grepl('errors',names(updated_records)))){
    print("Writing Error Log")
    updated_records$errors <- as.character(updated_records$errors)
  }
  
  updated_records$id <- upload$id
  
  # write the results
  print("Uploading Results")
  write_sheet(updated_records,
              upload.link,
              sheet = 'Upload Results')
  # print("Resizing Upload Result Cols")
  # range_autofit(upload.link,sheet = 'Upload Results')
  if(nchar(notes)>=50000){
    warning("notes longer than 50k, truncating")
    notes <- strtrim(notes,49999)
  }
  # upload notes if there are any
  if(!is.na(notes)){
    print("Writing Notes")
    write_sheet(data.frame(notes = notes,
                           row.names = FALSE),
                upload.link,
                sheet = 'Notes')
    range_autofit(upload.link,sheet = 'Notes')
  }
  print(paste0("Done, safe to check file @ ",upload.link))
}

# Add a variable to the environment for a list of the column names in Excel and Google Sheets
# this can be used to address columns based on their numeric position
# e.g., spreadsheet.cols[300] will give the 300th column name in a google sheet
spreadsheet.cols <- expand.grid(LETTERS, LETTERS)
spreadsheet.cols <- spreadsheet.cols[order(spreadsheet.cols$Var1,spreadsheet.cols$Var2),]
spreadsheet.cols <- c(LETTERS, do.call('paste0',spreadsheet.cols))

na.subtraction <- function(thing1,thing2){
  # a function to subtract 1 thing from another and treat any NA values as zeros
  # takes two vectors as input and returns one after subtraction
  thing1[which(is.na(thing1))] <- 0
  thing2[which(is.na(thing2))] <- 0
  return(thing1 - thing2)
}


AB_HEALTH_USER <- "hs_readonly"
AB_HEALTH_PASS <- "wE60kAjR7nuP"
CERBERUS_ORDER_MANAGER_USR <- "RevOps"
CERBERUS_ORDER_MANAGER_PASS <- "+%Xddjx^JF6^+W3R9VUA"
SSMS_USR <- "R"
SSMS_PASS <- "I Love Redwood!"


