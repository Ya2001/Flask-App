from flask import Flask, render_template, redirect, url_for, request, flash, send_from_directory
import pandas as pd 
import numpy as np
import xlsxwriter
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import os

# schedule_df = pd.read_csv('alarm_codes.xlsx', usecols= ['Error  Number', 'Error  Type'])
# schedule_df = schedule_df.dropna()
# schedule_df['Error  Number'] = schedule_df['Error  Number'].str.replace('Alm', '').astype('int64')

# Creating a flask web app instance
load_dotenv()
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'
UPLOAD_FOLDER = 'App Run Data'
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_raw_data(file_path, filename):

        # Read the raw data file
        raw_df = pd.read_excel(file_path)

        # Read the error codes schedule file
        schedule_df = pd.read_excel('alarm_codes.xlsx', usecols=['Error  Number', 'Error  Type'], engine='openpyxl')
        schedule_df = schedule_df.dropna()
        schedule_df['Error  Number'] = schedule_df['Error  Number'].str.replace('Alm', '').astype('int64')

        # Create a dictionary for error codes
        error_type_dict = dict(zip(schedule_df['Error  Number'], schedule_df['Error  Type']))

        # Extract and map error codes
        raw_df['First Error Code'] = raw_df['Codes'].str.split(';').str[0]
        raw_df['First Error Code'] = raw_df['First Error Code'].fillna(0).astype('int64')
        raw_df['Error Type'] = raw_df['First Error Code'].map(error_type_dict).fillna('Unknown')
        
        # Convert Date Time to datetime object
        raw_df['time utc'] = pd.to_datetime(raw_df['time utc'], format='%d/%m/%Y %H:%M:%S')

        # Sort raw_df by 'WTG Number' and 'time utc' to ensure correct order for instance calculation
        filtered_df = raw_df.sort_values(by=['WTG Number', 'time utc'])

        # Identify instances of each error
        filtered_df['Next Date Time'] = filtered_df['time utc'].shift(-1)
        filtered_df['Next Error Code'] = filtered_df['First Error Code'].shift(-1)
        filtered_df['Next WTG Number'] = filtered_df['WTG Number'].shift(-1)
        
        # Mark the start of a new error instance
        filtered_df['New Instance'] = (filtered_df['First Error Code'] != filtered_df['Next Error Code']) | \
                                (filtered_df['WTG Number'] != filtered_df['Next WTG Number']) | \
                                (filtered_df['Next Date Time'] - filtered_df['time utc'] != pd.Timedelta(minutes=10))

        # Create an 'Instance ID' column to group by
        filtered_df['Instance ID'] = (filtered_df['New Instance'].shift(1).fillna(True)).cumsum()

        # Calculate duration for each instance
        instance_durations = filtered_df.groupby('Instance ID').agg({
                'WTG Number': 'first',
                'time utc': 'first',
                'First Error Code': 'first',
                'Error Type': 'first',
                'Next Date Time': 'last'
        }).reset_index(drop=True)
        
        def calculate_duration(row):
                if pd.isnull(row['Next Date Time']):  # Handle NaN case, if necessary
                        return np.nan
                
                start_time = row['time utc']
                end_time = row['Next Date Time']
                # Check if end time is before start time (crossed month boundary)
                if end_time < start_time:
                        # Assuming we need to calculate until the end of the month
                        end_of_month = pd.Timestamp(start_time + pd.offsets.MonthEnd(0))
                        duration = (end_of_month - start_time).total_seconds() / 3600
                else:
                        duration = (end_time - start_time).total_seconds() / 3600
                        return duration

        # Apply the function to calculate Duration (Hours)
        instance_durations['Duration (Hours)'] = instance_durations.apply(calculate_duration, axis=1)

        # Optional: Round off Duration (Hours) to desired precision
        instance_durations['Duration (Hours)'] = instance_durations['Duration (Hours)'].round(2)

        # Drop rows with NaN durations, if any
        instance_durations = instance_durations.dropna(subset=['Duration (Hours)'])

        # Rename columns if necessary
        instance_durations = instance_durations.rename(columns={
        'time utc': 'Start Time',
        'First Error Code': 'Error Code',
        'Next Date Time': 'End Time',
        'Duration (Hours)': 'Duration (Hours)'
        })
        
        penalizing_sum = {}
        non_penalizing_sum = {}
        warning_sum = {}
        
        sums = {
        'penalizing': penalizing_sum,
        'non_penalizing': non_penalizing_sum,
        'warning': warning_sum
        }
        
        base_filename = os.path.splitext(filename)[0]
        filtered_filename = f"{base_filename}_filtered.xlsx"
        excel_path = os.path.join(app.config['UPLOAD_FOLDER'], filtered_filename)
        print(f"Saving processed raw data to: {excel_path}")
        
        
        # writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
        # Filter out unknown error types for the "Error Instances" sheet
        
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                instance_durations_filt = instance_durations[instance_durations['Error Code'] != 0]
                instance_durations_filt.to_excel(writer, index=False, sheet_name='Error Instances', header=True)

                for turbine_name, group_df in instance_durations.groupby('WTG Number'):
                        penalizing_downtime = group_df[group_df['Error Type'] == 1]
                        non_penalizing_downtime = group_df[group_df['Error Type'] == 0]
                        warning = group_df[group_df['Error Type'] == 'W']
                        unknown = group_df[group_df['Error Type'] == 'Unknown']

                        penalizing_downtime.to_excel(writer, index=False, sheet_name=f'{turbine_name}_Penalizing', header=True)
                        non_penalizing_downtime.to_excel(writer, index=False, sheet_name=f'{turbine_name}_Non_Penalizing', header=True)
                        warning.to_excel(writer, index=False, sheet_name=f'{turbine_name}_Warning', header=True)
                        unknown.to_excel(writer, index=False, sheet_name=f'{turbine_name}_Unknown', header=True)
                        
                        group_df = group_df[group_df['Error Type'] != 'Unknown']

                        duration_sums = group_df.groupby('Error Type')['Duration (Hours)'].sum()
                        sums_df = duration_sums.reset_index(name='Total Duration')
                        sums_df.to_excel(writer, index=False, sheet_name=f'{turbine_name}_Duration_Sums', header=True)

        return filtered_filename



def process_alarm_log(file_path, filename):
        print(f"Processing alarm log for file: {filename}")
        alarm_log = pd.read_excel(file_path, skiprows=2)
        alarm_log['From'] = pd.to_datetime(alarm_log['From'], format='mixed')
        alarm_log['To'] = pd.to_datetime(alarm_log['To'], format='mixed')
        #writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')

        alarm_log = alarm_log.dropna(subset=['To'])
        alarm_log_sorted = alarm_log.sort_values(by='From')

        time_window = pd.Timedelta(minutes=10)

        start_time = None
        end_time = None
        selected_errors = []

        # Iterate over each row in the sorted data
        for _, row in alarm_log_sorted.iterrows():
                if start_time is None:
                        start_time = row['From']
                        end_time = row['To']
                else:
                        if row['From'] - start_time <= time_window:
                        # If the error occurs within the time window, extend the end time
                                end_time = max(end_time, row['To'])
                        else:
                                # If the error is outside the time window, select the previous error
                                selected_errors.append(selected_error)
                                # Start a new time window with the current error
                                start_time = row['From']
                                end_time = row['To']
                selected_error = row
        # Select the last error
        if selected_error is not None:
                selected_errors.append(selected_error)

        base_filename = os.path.splitext(filename)[0]
        filtered_filename = f"{base_filename}_filtered.xlsx"
        excel_path = os.path.join(app.config['UPLOAD_FOLDER'], filtered_filename)
        print(f"Saving processed alarm log to: {excel_path}")

        selected_errors_df = pd.DataFrame(selected_errors)


        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                selected_errors_df.to_excel(writer, index=False, header=True)

        selected_errors_df.to_excel(writer, index=False, header=True)

        return filtered_filename

@app.route('/', methods=['GET', 'POST'])
def upload_file():
        if request.method != 'POST':
                return render_template('index.html')
        if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
        if not file or not allowed_file(file.filename):
                return render_template('error.html', message='Invalid file format')
        filename = secure_filename(file.filename)
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        print(f"Saving uploaded file to: {save_path}")
        file.save(save_path)
        if 'raw_data' in filename:
                result = process_raw_data(filename)
        elif 'alarm_log' in filename:
                result = process_alarm_log(filename)
        else:
                result = 'File uploaded successfully'
        return render_template('success.html', message=result)

@app.route('/upload_raw_data', methods=['POST'])
def upload_raw_data():
        if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
        
        file = request.files['file']
        custom_filename = request.form.get('custom_filename')
        if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
        
        if file and allowed_file(file.filename):
                filename = secure_filename(custom_filename) if custom_filename else secure_filename(file.filename)
                save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                print(f"Saving uploaded raw data file to: {save_path}")
                file.save(save_path)
                # Call the processing function after the file is saved
                result = process_raw_data(save_path, filename)
                
                # Return a response indicating success
                return render_template('success.html', message=result)
        else:
                return render_template('error.html', message='Invalid file format')

@app.route('/upload_alarm_log', methods=['POST'])
def upload_alarm_log():
        if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
        
        file = request.files['file']
        custom_filename = request.form.get('custom_filename')
        if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
        
        if file and allowed_file(file.filename):
                filename = secure_filename(custom_filename) if custom_filename else secure_filename(file.filename)
                save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                print(f"Saving uploaded alarm log file to: {save_path}")
                file.save(save_path)
                
                # Call the processing function after the file is saved
                result = process_alarm_log(save_path, filename)
                
                # Return a response indicating success
                return render_template('success.html', message=result)
        else:
                return render_template('error.html', message='Invalid file format')

@app.route('/download/<filename>')
def download_file(filename):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
        app.run()