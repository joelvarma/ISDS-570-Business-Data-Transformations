# Stock Market Case in R
rm(list=ls(all=T)) # this just removes everything from memory

dp<-read.csv('/Users/joelvarma/Dropbox (CSU Fullerton)/SHARED/data/export_daily_prices_2012_2017.csv') # no arguments

#Explore
head(dp) #first few rows
tail(dp) #last few rows
nrow(dp) #row count

#remove the last two rows (because they were empty/errors)
dp<-head(dp,-1)

#to exclude any errors
dp[which(dp$symbol=='ZUMZ'),]

#This is an easy way (csv) but we are not going to use it here
rm(dp) # remove from memory
#We are going to perform most of the transformation tasks in R

# Connect to PostgreSQL ---------------------------------------------------

# Make sure you have created the reader role for our PostgreSQL database
# and granted that role SELECT rights to all tables
# Also, make sure that you have completed (or restored) Part 3b db

require(RPostgreSQL) # did you install this package?
require(DBI)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="stockmarketreader"
                 ,password="read123"
                 ,host="localhost"
                 ,port=5433
                 ,dbname="stockmarket"
)
#custom calendar

qry='SELECT * FROM custom_calendar ORDER by date'
ccal<-dbGetQuery(conn,qry)
ccal
#eom prices and indices

qry1="SELECT symbol,date,adj_close FROM eod_indices WHERE date BETWEEN '2011-12-30' AND '2017-12-31'"
qry2="SELECT ticker,date,adj_close FROM eod_quotes WHERE date BETWEEN '2011-12-30' AND '2017-12-31'"

#qry3="SELECT symbol,date,adj_close FROM export_monthly_prices_2012_2017"

eom1<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
eom1
dbDisconnect(conn)
nrow(eom1)
eom1<-head(eom1,-1)
tail(eom1)

#######################################################################
#qry4="SELECT symbol,date,adj_close FROM export_monthly_prices_2012_2017 WHERE symbol like 'SP500TR%'"
#eom2<-dbGetQuery(conn,qry4)
#eom2

#nrow(eom2)
#head(eom2)
#tail(eom2)

#nrow(eom2)
#nrow(eom1)

#head(eom2[which(eom2$symbol=='SP500TR'),])


#Explore
head(ccal)
tail(ccal)
nrow(ccal)


head(eom1[which(eom1$symbol=='SP500TR'),])


#For monthly we may need one more data item (for 2011-12-30)
#We can add it to the database (INSERT INTO) - but to practice:

eom2_row<-data.frame(symbol='SP500TR',date=as.Date('2011-12-30'),adj_close=2158.94)
eom1<-rbind(eom1,eom2_row)
tail(eom1)
nrow(eom1)


tmonths1<-ccal[which(ccal$trading==1 & ccal$eom==1), ,drop=F]
head(tmonths1)
nrow(tmonths1)-1

nrow(tmonths1)

# Completeness ----------------------------------------------------------
# Percentage of completeness
#pct<-table(eom2$symbol)/(nrow(tmonths)-1)
#selected_symbols_monthly<-names(pct)[which(pct>=0.99)]
#length(selected_symbols_monthly)

#eom2_complete<-eom2[which(eom2$symbol %in% selected_symbols_monthly),,drop=F]


pct<-table(eom1$symbol)/(nrow(tmonths1)-1)
selected_symbols_monthly1<-names(pct)[which(pct>=0.99)]
length(selected_symbols_monthly1)


#eom1_complete<-eom1[which(eom1$symbol) %in% "SELECT symbol, date, adj_close FROM pct WHERE symbol likw 'R%'",,drop=F]
eom1_complete<-eom1[which(eom1$symbol %in% selected_symbols_monthly1),,drop=F]

#check

#head(eom2_complete)
#tail(eom2_complete)
#nrow(eom2_complete)


head(eom1_complete)
tail(eom1_complete)
nrow(eom1_complete)



#YOUR TURN: perform all these operations for monthly data
#Create eom and eom_complete
#Hint: which(ccal$trading==1 & ccal$eom==1)

require(reshape2) #did you install this package?
#eom_pvt<-dcast(eom2_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)


eom1_pvt<-dcast(eom1_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)

#check
eom1_pvt[1:10,1:5] #first 10 rows and first 2 columns 
ncol(eom1_pvt) # column count
nrow(eom1_pvt)

# Merge with Calendar -----------------------------------------------------
#eom_pvt_complete<-merge.data.frame(x=tmonths[,'date',drop=F],y=eom_pvt,by='date',all.x=T)

eom1_pvt_complete<-merge.data.frame(x=tmonths1[,'date',drop=F],y=eom1_pvt,by='date',all.x=T)

#check
#eom_pvt_complete[1:10,1:2] #first 10 rows and first 2 columns 
#ncol(eom_pvt_complete)
#nrow(eom_pvt_complete)

eom1_pvt_complete[1:10,1:5] #first 10 rows and first 2 columns 
ncol(eom1_pvt_complete)
nrow(eom1_pvt_complete)

#use dates as row names and remove the date column
#rownames(eom_pvt_complete)<-eom_pvt_complete$date
#eom_pvt_complete$date<-NULL

rownames(eom1_pvt_complete)<-eom1_pvt_complete$date
eom1_pvt_complete$date<-NULL


#re-check
#eom_pvt_complete[1:10,1:1] #first 10 rows and first 1 columns 
#ncol(eom_pvt_complete)
#nrow(eom_pvt_complete)

eom1_pvt_complete[1:10,1:4] #first 10 rows and first 1 columns 
ncol(eom1_pvt_complete)
nrow(eom1_pvt_complete)

# Missing Data Imputation -----------------------------------------------------
# We can replace a few missing (NA or NaN) data items with previous data
# Let's say no more than 3 in a row...
require(zoo)
#eom_pvt_complete<-na.locf(eom_pvt_complete,na.rm=F,fromLast=F,maxgap=3)

eom1_pvt_complete<-na.locf(eom1_pvt_complete,na.rm=F,fromLast=F,maxgap=5)
#re-check
#eom_pvt_complete[1:10,1:1] #first 10 rows and first 1 columns 
#ncol(eom_pvt_complete)
#nrow(eom_pvt_complete)

eom1_pvt_complete[1:10,1:4] #first 10 rows and first 1 columns 
ncol(eom1_pvt_complete)
nrow(eom1_pvt_complete)

# Calculating Returns -----------------------------------------------------
require(PerformanceAnalytics)
#eom_ret<-CalculateReturns(eom_pvt_complete)

eom1_ret<-CalculateReturns(eom1_pvt_complete)

#check
#eom_ret[1:10,1:1] #first 10 rows and first 1 columns 
#ncol(eom_ret)
#nrow(eom_ret)

eom1_ret[1:10,1:4] #first 10 rows and first 1 columns 
ncol(eom1_ret)
nrow(eom1_ret)

#remove the first row
#eom_ret<-tail(eom_ret,-1) #use tail with a negative value

eom1_ret<-tail(eom1_ret,-1) #use tail with a negative value

#check
#eom_ret[1:72,1:1] #first 10 rows and first 1 columns 
#ncol(eom_ret)
#nrow(eom_ret)

eom1_ret[1:10,1:4] #first 10 rows and first 1 columns 
ncol(eom1_ret)
nrow(eom1_ret)

###################################################################
# Check for extreme returns -------------------------------------------
# There is colSums, colMeans but no colMax so we need to create it
colMax <- function(data) sapply(data, max, na.rm = TRUE)
# Apply it
#max_monthly_ret<-colMax(eom_ret)
#max_monthly_ret[1:1] #first 10 max returns

max_monthly_ret1<-colMax(eom1_ret)
max_monthly_ret1[1:10] #first 10 max returns
# And proceed just like we did with percentage (completeness)
#selected_symbols_monthly<-names(max_monthly_ret)[which(max_monthly_ret<=1.00)]
#length(selected_symbols_monthly)

selected_symbols_monthly1<-names(max_monthly_ret1)[which(max_monthly_ret1<=1.00)]
length(selected_symbols_monthly1)

#subset eom_ret
#eom_ret<-eom_ret[,which(colnames(eom_ret) %in% selected_symbols_monthly)]

eom_ret1<-eom1_ret[,which(colnames(eom1_ret) %in% selected_symbols_monthly1)]
#check
#eom_ret[1:10,1:1] #first 10 rows and first 1 columns 
#ncol(eom_ret)
#nrow(eom_ret)

eom1_ret[1:10,1:4] #first 10 rows and first 1 columns 
ncol(eom1_ret)
nrow(eom1_ret)

###################################################################

# Export data from R to CSV -----------------------------------------------
write.csv(eom1_ret,'C:/Users/reddy/Desktop/ISDS 570/Part 3/R file/eom_R_final_ret.csv')

# You can actually open this file in Excel!


# Tabular Return Data Analytics -------------------------------------------

# We will select 'SP500TR' and c('AEGN','AAON','AMSC','ALCO','AGNC','AREX','ABCB','ABMD','ACTG','ADTN','AAPL','AAL')
# We need to convert data frames to xts (extensible time series)
Ra<-as.xts(eom1_ret[,c('D','DAKT','DAL'),drop=F])
Rb<-as.xts(eom1_ret[,'SP500TR',drop=F]) #benchmark

head(Ra)
head(Rb)

# And now we can use the analytical package...

# Stats
table.Stats(Ra)

table.Stats(Rb)

# Distributions
table.Distributions(Ra)

table.Distributions(Rb)

# Returns
table.AnnualizedReturns((Rb),scale=12)

table.AnnualizedReturns((Ra),scale=12)

table.AnnualizedReturns(cbind(Rb,Ra),scale=12) # note for monthly use scale=12

# Accumulate Returns
acc_Ra<-Return.cumulative(Ra)
acc_Rb<-Return.cumulative(Rb)

# Capital Assets Pricing Model
table.CAPM(Ra,Rb)

# YOUR TURN: try other tabular analyses

# Graphical Return Data Analytics -----------------------------------------

# Cumulative returns chart
chart.CumReturns(Ra,legend.loc = 'topleft')
chart.CumReturns(Rb,legend.loc = 'topleft')

chart.CumReturns(cbind(Ra,Rb),legend.loc = 'topleft')

#Box plots
# chart.Boxplot(cbind(Rb,Ra))
# 
# chart.Drawdown(Ra,legend.loc = 'bottomleft')
# 
