from collections import namedtuple
import xml.sax
from datetime import datetime
import zipfile
from contextlib import contextmanager

import pandas as pd

import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandSystemExit, CommandUIRender)

ExtractionResult = namedtuple("ExtractionResult", ["id", "title", "data_frame"])

filter_start_date = datetime(2017, 1, 1)


class FileInZipNotFoundError(Exception):
    """Raised when a specific file is not found within the ZIP archive."""

class EmptyHealthDataError(Exception):
    """Raised when there are no health data records in the XML."""

class InvalidXMLError(Exception):
    """Raised when the XML input is invalid or empty."""


class HealthDataHandler(xml.sax.ContentHandler):
    def __init__(self, callback):
        self.callback = callback

    def startElement(self, tag, attributes):
        if tag == "Record" and attributes["type"] == "HKQuantityTypeIdentifierStepCount":
            value = int(attributes["value"])
            start_date = self.parse_naive_datetime(attributes["startDate"])
            self.callback(value, start_date)
            
    def parse_naive_datetime(self, date_str):
        dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %z')
        return dt.replace(tzinfo=None)

class StepCountCallback:
    def __init__(self):
        self.start_times = []
        self.steps = []

    def __call__(self, value, start_date):
        if start_date < filter_start_date:
            return
        self.steps.append(value)
        self.start_times.append(start_date)

    def to_dataframe(self):
        return pd.DataFrame({'Start Time': self.start_times, 'Steps': self.steps})

def parse_health_data(file_obj):
    callback_instance = StepCountCallback()

    parser = xml.sax.make_parser()
    handler = HealthDataHandler(callback_instance)
    parser.setContentHandler(handler)

    try:
        parser.parse(file_obj)
    except xml.sax.SAXParseException:
        raise InvalidXMLError("The provided XML input is invalid or empty.")

    df = callback_instance.to_dataframe()

    if df.empty:
        raise EmptyHealthDataError("No health data records found in the XML input.")

    return df


def aggregate_daily_steps(file_obj):
    # Parse the XML to get initial DataFrame
    df = parse_health_data(file_obj)
    
    # Extract just the date from 'Start Time' column
    df['Date'] = df['Start Time'].dt.strftime("%Y-%m-%d")
    
    # Group by this date and sum the steps
    daily_steps = df.groupby('Date')['Steps'].sum().reset_index()
    
    return daily_steps


@contextmanager
def open_export_zip(zip_path, file_name="apple_health_export/export.xml"):
    archive = zipfile.ZipFile(zip_path, 'r')
    if file_name not in archive.namelist():
        archive.close()
        raise FileInZipNotFoundError(f"'{file_name}' was not found in the ZIP archive.")
        
    try:
        yield archive.open(file_name)
    finally:
        archive.close()

def aggregate_steps_from_zip(zip_path):
    with open_export_zip(zip_path) as f:
        return aggregate_daily_steps(f)
    
def extract_daily_steps_from_zip(zip_path):
    step_data = aggregate_steps_from_zip(zip_path)
    return ExtractionResult(
    "ihealth_step_counts",
    props.Translatable(
        {"en": "Steps", "nl": "Stappen"}
    ),
    pd.DataFrame(step_data),
)



def process(sessionId):
    yield donate(f"{sessionId}-tracking", '[{ "message": "user entered script" }]')

    platforms = ["Twitter", "Facebook", "Instagram", "Youtube"]

    # STEP 1: select the file
    data = None
    while True:
        promptFile = prompt_file( )
        fileResult = yield render_donation_page(promptFile, 33)
        if fileResult.__type__ == 'PayloadString':
            meta_data.append(("debug", f"extracting file"))
            extractionResult = extract_daily_steps_from_zip(fileResult.value)
            if extractionResult != 'invalid':
                meta_data.append(("debug", f"extraction successful, go to consent form"))
                data = extractionResult
                break
            else:
                meta_data.append(("debug", f"prompt confirmation to retry file selection"))
                retry_result = yield render_donation_page(retry_confirmation(), 33)
                if retry_result.__type__ == 'PayloadTrue':
                    meta_data.append(("debug", f"skip due to invalid file"))
                    continue
                else:
                    meta_data.append(("debug", f"retry prompt file"))
                    break
                else:
                    meta_data.append(("debug", f"{platform}: prompt confirmation to retry file selection"))
                    retry_result = yield render_donation_page(platform, retry_confirmation(platform), progress)
                    if retry_result.__type__ == 'PayloadTrue':
                        meta_data.append(("debug", f"{platform}: skip due to invalid file"))
                        continue
                    else:
                        meta_data.append(("debug", f"{platform}: retry prompt file"))
                        break
            else:
                meta_data.append(("debug", f"{platform}: skip to next step"))
                break

        # STEP 2: ask for consent
        progress += step_percentage
        if data is not None:
            meta_data.append(("debug", f"{platform}: prompt consent"))
            prompt = prompt_consent(platform, data, meta_data)
            consent_result = yield render_donation_page(platform, prompt, progress)
            if consent_result.__type__ == "PayloadJSON":
                meta_data.append(("debug", f"{platform}: donate consent data"))
                yield donate(f"{sessionId}-{platform}", consent_result.value)

    yield exit(0, "Success")
    yield render_end_page()


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({
        "en": platform,
        "nl": platform
    }))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation():
    text = props.Translatable({
        "en": f"Unfortunately, we cannot process your {platform} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
        "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
    })
    ok = props.Translatable({
        "en": "Try again",
        "nl": "Probeer opnieuw"
    })
    cancel = props.Translatable({
        "en": "Continue",
        "nl": "Verder"
    })
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(platform, extensions):
    description = props.Translatable({
        "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a {platform} file. ",
        "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
    })

    return props.PropsUIPromptFileInput(description, extensions)


def doSomethingWithTheFile(platform, filename):
    return extract_zip_contents(filename)


def extract_zip_contents(filename):
    names = []
    try:
        file = zipfile.ZipFile(filename)
        data = []
        for name in file.namelist():
            names.append(name)
            info = file.getinfo(name)
            data.append((name, info.compress_size, info.file_size))
        return data
    except zipfile.error:
        return "invalid"


def prompt_consent(id, data, meta_data):

    table_title = props.Translatable({
        "en": "Zip file contents",
        "nl": "Inhoud zip bestand"
    })

    log_title = props.Translatable({
        "en": "Log messages",
        "nl": "Log berichten"
    })

    data_frame = pd.DataFrame(data, columns=["filename", "compressed size", "size"])
    table = props.PropsUIPromptConsentFormTable("zip_content", table_title, data_frame)
    meta_frame = pd.DataFrame(meta_data, columns=["type", "message"])
    meta_table = props.PropsUIPromptConsentFormTable("log_messages", log_title, meta_frame)
    return props.PropsUIPromptConsentForm(tables, [meta_table])


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)
