# task_scheduler.py

import pythoncom
import win32com.client
import datetime

def get_all_tasks(desired_names=None):
    pythoncom.CoInitialize()  # Initialize COM
    scheduler = win32com.client.Dispatch('Schedule.Service')
    scheduler.Connect()
    
    root_folder = scheduler.GetFolder('\\')
    tasks = root_folder.GetTasks(1) 
    
    def status_to_string(status_code):
        status_map = {
            1: 'Disabled',
            2: 'Running',
            3: 'Ready',
            4: 'Running',
            5: 'Unknown'
        }
        return status_map.get(status_code, 'Unknown')

    def parse_datetime(dt_str):
        try:
            return datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None

    def get_trigger_details(triggers):
        trigger_details = []
        
        days_of_week = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        months_of_year_abbr = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for trigger in triggers:
            if trigger.Type == 1:  # TimeTrigger
                start_time = parse_datetime(trigger.StartBoundary)
                if start_time:
                    trigger_details.append(f'One Time at {start_time.strftime("%I:%M %p")}')
                    
            elif trigger.Type == 2:  # DailyTrigger
                interval = trigger.DaysInterval
                start_time = parse_datetime(trigger.StartBoundary)
                if start_time:
                    trigger_details.append(f'Daily every {interval} day(s) at {start_time.strftime("%I:%M %p")}')
                    
            elif trigger.Type == 3:  # WeeklyTrigger
                days = [days_of_week[i] for i in range(7) if (trigger.DaysOfWeek & (1 << i)) != 0]
                start_time = parse_datetime(trigger.StartBoundary)
                weeks_interval = getattr(trigger, 'WeeksInterval', 1)  # Default to 1 if WeeksInterval is not available
                if start_time:
                    trigger_details.append(f'Weekly every {weeks_interval} week(s) on {", ".join(days)} at {start_time.strftime("%I:%M %p")}')
                    
            elif trigger.Type == 4:  # MonthlyTrigger
                months_str = ', '.join(months_of_year_abbr[i] for i in range(12) if (trigger.MonthsOfYear & (1 << i)) != 0)
                days_str = ', '.join(str(i+1) for i in range(31) if (trigger.DaysOfMonth & (1 << i)) != 0)
                start_time = parse_datetime(trigger.StartBoundary)
                if start_time:
                    trigger_details.append(f'Monthly on {days_str} day(s) of {months_str} at {start_time.strftime("%I:%M %p")}')      
            else:
                trigger_details.append('Unknown Trigger')
        return ', '.join(trigger_details)
    task_list = []
    for task in tasks:
        if desired_names and task.Name not in desired_names:
            continue
        last_run_time = task.LastRunTime
        triggers_info = get_trigger_details(task.Definition.Triggers)
        task_info = {
            'name': task.Name,
            'next_run_time': task.NextRunTime.strftime('%Y-%m-%d %H:%M:%S') if task.NextRunTime else 'N/A',
            'last_run_time': last_run_time.strftime('%Y-%m-%d %H:%M:%S') if last_run_time else 'N/A',
            'status': status_to_string(task.State),
            'triggers': triggers_info
        }
        task_list.append(task_info)
    
    return task_list


def update_task(name, new_name, new_start_time, trigger_type, days_interval=None, days_of_week=None, days_of_month=None, months_of_year=None, weeks_interval=None):
    pythoncom.CoInitialize()
    scheduler = win32com.client.Dispatch('Schedule.Service')
    scheduler.Connect()
    
    root_folder = scheduler.GetFolder('\\')
    task = root_folder.GetTask(name)
    
    if not task:
        raise ValueError(f'Task with name "{name}" not found.')

    task_def = task.Definition
    task_def.RegistrationInfo.Description = new_name

    triggers = task_def.Triggers
    
    # Clear existing triggers
    while len(triggers) > 0:
        triggers.Remove(1)
    
    # Create a new trigger based on the selected type
    if trigger_type == '1':  # One Time
        new_trigger = task_def.Triggers.Create(1)
        new_trigger.StartBoundary = new_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    
    elif trigger_type == '2':  # Daily
        new_trigger = task_def.Triggers.Create(2)
        new_trigger.StartBoundary = new_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        new_trigger.DaysInterval = days_interval
    
    elif trigger_type == '3':  # Weekly
        new_trigger = task_def.Triggers.Create(3)
        new_trigger.StartBoundary = new_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        days_mask = sum([1 << (int(day) - 1) for day in days_of_week])
        new_trigger.DaysOfWeek = days_mask
        new_trigger.WeeksInterval = weeks_interval
    
    elif trigger_type == '4':  # Monthly
        new_trigger = task_def.Triggers.Create(4)
        new_trigger.StartBoundary = new_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        if days_of_month:
            print(f'Received Days of Month: {days_of_month}')  # Debug output
            day_mask = sum([1 << (int(day) - 1) for day in days_of_month])
            new_trigger.DaysOfMonth = day_mask
            print(f'Computed Day Mask: {bin(day_mask)}')  # Debug output
        
        if months_of_year:
            print(f'Received Months of Year: {months_of_year}')  # Debug output
            month_mask = sum([1 << (int(month) - 1) for month in months_of_year])
            new_trigger.MonthsOfYear = month_mask
            print(f'Computed Month Mask: {bin(month_mask)}')  # Debug output
    
    else:
        raise ValueError('Invalid trigger type.')

    root_folder.RegisterTaskDefinition(
        name,
        task_def,
        6,  # Replace the task if it already exists
        None,
        None,
        3,  # Use the current user context
    )
    
    print(f'Task "{name}" updated successfully.')
    print(f'Days of Month: {days_of_month}')
    print(f'Months of Year: {months_of_year}')
