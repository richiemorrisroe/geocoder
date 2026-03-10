require(testthat)
options(warn=-1)
args <- commandArgs(trailingOnly=TRUE)
output_old  <-  suppressMessages(readr::read_csv(args[[1]]))
output_new <- suppressMessages(readr::read_csv(args[[2]]))
print(test_that("sample files are equal", 
                expect_equal(output_old, output_new)))
