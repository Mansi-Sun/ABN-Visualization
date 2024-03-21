## Australia Registered Businesses Statistic

### Target: In this project, I would like to analysis the business registration in Australia year over year. In my data visualization part, I would like to answer below questions:

- 1. Last 10 years ABN statistic in Australia

- 2. The fastest-growing business state in Australia

- 3. Top 10 business entity type in Australia

### Data Source:

[1. Business Names](https://data.gov.au/data/dataset/asic-business-names)

**File downloaded:**
    Business Names Dataset - Help File & Business Names Dataset - Current

> [!NOTE]
> The data set contains below information:
>   * Business Name
>   * Status
>   * Date of registration
>   * Date of Cancellation
>   * Renewal Date
>   * Former State Number(where applicable)
>   * Previous State of Registration
>   * Australia Business Number(ABN)
> 
> As above data is not sufficient enough for the later on agrregation and visualization, so we could enrich our dataset by adding Entity type and state information, which could be fetched in below link.

[2. Entity Type & State](https://data.gov.au/data/dataset/abn-bulk-extract)

**File downloaded:**
    ABN Bulk Extract Part1 & ABN Bulk Extract Part2

> [!NOTE]
> The files are XML files which will need more than 10GB disk space. To make things simple, I only extracted the columns (ABN,Entity type,State) we needed, and converted them into parquet files for later use. 
>
> The convert script located in "Code/Convert_XML_to_Parquet.py" 
>
> The 20 converted files located in "Data/*.parquet" 
