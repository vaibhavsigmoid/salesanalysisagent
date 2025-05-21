rm *.csv 
python prep_raw_data.py
python prep_modified_raw_data.py
cp raw_pos.csv ../data/combined_testcase/
