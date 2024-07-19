import dataclasses
from pyflink.common import Types

@dataclasses.dataclass
class tripUpdates:
    @staticmethod
    def vehicleTypeInfo():
        return Types.ROW_NAMED(['id', 'label', 'license_plate'], 
                               [Types.STRING(), Types.STRING(), Types.STRING()])

    @staticmethod
    def arrivalDepartureType():
        return Types.ROW_NAMED(['delay', 'time', 'uncertainty'], 
                               [Types.INT(), Types.INT(), Types.INT()])

    @staticmethod
    def stopTimeUpdateType(arrivalDepartureType=arrivalDepartureType):
        return Types.ROW_NAMED(['stop_sequence', 'arrival', 'departure', 'stop_id', 'schedule_relationship'], 
                               [Types.INT(), arrivalDepartureType, arrivalDepartureType, Types.STRING(), Types.INT()])

    @staticmethod
    def tripUpdateTripInfo():
        return Types.ROW_NAMED(['trip_id', 'start_time', 'start_date', 'schedule_relationship', 'route_id', 'direction_id'], 
                               [Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT()])
    
    @staticmethod
    def tripUpdateType(tripUpdateTripInfo=tripUpdateTripInfo, stopTimeUpdateType = stopTimeUpdateType):
        return Types.ROW_NAMED(['trip', 'stop_time_update', 'vehicle', 'timestamp', 'delay'],
                               [tripUpdateTripInfo, Types.OBJECT_ARRAY(stopTimeUpdateType), Types.STRING(), Types.STRING(), Types.INT()])
    
    @staticmethod
    def tripUpdateRow(tripUpdateType = tripUpdateType):
        return Types.ROW_NAMED(['id', 'is_deleted', 'trip_update', 'vehicle', 'alert'], 
                               [Types.STRING(), Types.BOOLEAN(), tripUpdateType, Types.STRING(), Types.STRING()])
    