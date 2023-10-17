import csv

# Define the data
txt_data = [["01/2016", "56596270931.31685"], ["01/2017", "22507570807.719795"], ["01/2018", "52106060636.84508"], ["01/2019", "13954460713.077589"], ["02/2016", "69180681134.38847"], ["02/2017", "23047230327.254307"], ["02/2018", "23636574203.828976"], ["03/2016", "32797039087.356663"], ["03/2017", "23232253600.81683"], ["03/2018", "15549765961.743275"], ["04/2016", "23361180502.721268"], ["04/2017", "22355124545.395317"], ["04/2018", "13153739247.92998"], ["05/2016", "23746277028.26325"], ["05/2017", "23572314972.015255"], ["05/2018", "17422505108.986423"], ["06/2016", "23021251389.81213"], ["06/2017", "30199442465.12872"], ["06/2018", "16533308366.813038"], ["07/2016", "22629542449.24175"], ["07/2017", "25460300456.232983"], ["07/2018", "27506077453.154327"], ["08/2015", "159744029578.0331"], ["08/2016", "22396836435.95849"], ["08/2017", "25905774673.990215"], ["08/2018", "18483235826.894566"], ["09/2015", "56511301521.033226"], ["09/2016", "25270403393.626087"], ["09/2017", "30675032016.988663"], ["09/2018", "15213870989.523373"], ["10/2015", "53901692120.53661"], ["10/2016", "32112869584.91466"], ["10/2017", "17498286426.76892"], ["10/2018", "14526936383.350008"], ["11/2015", "53607614201.796776"], ["11/2016", "24634294365.279957"], ["11/2017", "15312465314.69355"], ["11/2018", "16034859008.681646"], ["12/2015", "55899526672.35486"], ["12/2016", "50318068074.68645"], ["12/2017", "33439362876.108322"], ["12/2018", "16338844844.014647"]]

# Open the output .csv file for writing
with open('avg_price.csv', 'w', newline='') as csv_file:
    # Create a CSV writer object
    writer = csv.writer(csv_file)

    # Write the header row
    writer.writerow(['Date', 'Average gas price'])

    # Loop through the data
    for row in txt_data:
        # Write the row to the .csv file
        writer.writerow(row)