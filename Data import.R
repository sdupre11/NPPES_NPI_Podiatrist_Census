#The NPI file is way too big to work with whole cloth, so I bring it in and then cut it up into batches of 1,000,000 rows

library(tidyverse)
library(arrow)
df <- read_csv_arrow("C:/Users/sdupre.APMA/OneDrive - APMA/Desktop/Reference Materials/Data/NPI Registry/NPPES_Data_Dissemination_June_2025_V2/npidata_pfile_20050523-20250608.csv") %>%
  as.data.frame()


n_rows <- nrow(df)
chunk_size <- 1e6
n_chunks <- ceiling(n_rows / chunk_size)

# Example: Save each chunk as a separate RDS file
for (i in seq_len(n_chunks)) {
  idx <- ((i - 1) * chunk_size + 1):min(i * chunk_size, n_rows)
  chunk <- df[idx, ]
  
  saveRDS(chunk, file = sprintf("C:/Users/sdupre.APMA/OneDrive - APMA/Desktop/Reference Materials/Data/NPI Registry/processed/NPI_20050523-20250608_chunk_%02d.rds", i))
}

rm(df, n_rows, chunk_size, n_chunks, chunk, i, idx)

NPI_chunk_1 <- readRDS("C:/Users/sdupre.APMA/OneDrive - APMA/Desktop/Reference Materials/Data/NPI Registry/processed/NPI_20050523-20250608_chunk_01.rds")
NPI_chunk_1 %>% 
  glimpse()
rm(NPI_chunk_1)

#
#write_feather(df, "C:/Users/sdupre.APMA/OneDrive - APMA/Desktop/Reference Materials/Data/NPI Registry/NPPES_Data_Dissemination_June_2025_V2/npidata_pfile_20050523-20250608.feather")
