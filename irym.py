import pydicom
from pydicom.dataset import Dataset
from pynetdicom import AE, QueryRetrievePresentationContexts, debug_logger

import logging
import traceback


logging.basicConfig(
    level=logging.INFO,
    filemode='a',
    filename='IRYM_logging'
)


debug_logger()


def c_find_query():
    """
    Выполняет C-FIND запрос и возвращает study_instance_uid, series_instance_uid
    найденных исследований по параметрам датасета
    """
    logging.info("c_find_query func")
    ae = AE()
    ae.requested_contexts = QueryRetrievePresentationContexts

    ds = Dataset()
    ds.QueryRetrieveLevel = 'SERIES'
    ds.Modality = 'MG'

    try:
        association = ae.associate('127.0.0.1', 4242)
        logging.info(f"association created: {association.is_established}")
        if association.is_established:
            results = association.send_c_find(ds, query_model='S')
            logging.info("c-find requested")
            for (status, series) in results:
                if status.Status == 0xFF00:
                    logging.info(f"status: {status.Status}")
                    series_instance_uid = series.SeriesInstanceUID
                    study_instance_uid = series.StudyInstanceUID
                    break
            association.release()
            logging.info("association released")
            return study_instance_uid, series_instance_uid
        else:
            logging.info("association unsuccessful, shutdown")
            return "error"
    except Exception as err:
        logging.info(f"association error: {err}")
        logging.info(traceback.format_exc())
        return "error"


def create_rotated_series(study_instance_uid, series_instance_uid):
    """
    Получает первую серию исследований.
    Создаёт датасет с копиями изображений, повёрнутых на 90 градусов.
    Загружает новую серию на локальный сервер.
    """
    logging.info("create_rotated_series func")

    ae = AE()
    ae.requested_contexts = QueryRetrievePresentationContexts

    try:
        association = ae.associate('127.0.0.1', 4242)
        logging.info(f"association created: {association.is_established}")
        if association.is_established:
            ds = association.send_c_get(
                study_instance_uid, series_instance_uid)
            logging.info("c-get requested")
            for (status, dataset) in ds:
                if status.Status == 0xFF00:
                    logging.info(f"status: {status.Status}")
                    rotated_dataset = []
                    for image in dataset:
                        rotated_image = pydicom.dataset.Dataset()
                        rotated_image.update(image)
                        rotated_image.rotate(90)
                        rotated_dataset.append(rotated_image)

                    new_series_instance_uid = pydicom.uid.generate_uid()
                    new_series_description = 'Rotated series'
                    new_series = pydicom.dataset.Dataset()
                    new_series.SeriesInstanceUID = new_series_instance_uid
                    new_series.SeriesDescription = new_series_description
                    new_series.Modality = 'MG'
                    new_series.Images = rotated_dataset

                    association.send_c_store(new_series, study_instance_uid)
                    logging.info("c_store requested")

            association.release()

    except Exception as err:
        logging.info(f"association error: {err}")
        logging.info(traceback.format_exc())
        return "error"


if __name__ == '__main__':
    study_instance_uid, series_instance_uid = c_find_query()
    if study_instance_uid and series_instance_uid:
        if study_instance_uid != "error":
            create_rotated_series(study_instance_uid, series_instance_uid)
    else:
        logging.info("not found")
