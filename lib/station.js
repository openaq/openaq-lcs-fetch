'use strict';

class SensorNode {
    constructor() {
        self.sensor_node_id = null;
        self.sensor_node_site_name = null;
        self.sensor_node_site_description = null;
        self.sensor_node_deployed_by = null;
        self.sensor_node_deployed_date = null;
        self.sensor_node_deploy_notes = null;
        self.sensor_node_ismobile = null;
        self.sensor_node_geometry = null;
        self.sensor_node_timezone = null;
        self.sensor_node_reporting_frequency = null;
        self.sensor_node_city = null;
        self.sensor_node_country = null;

        self.sensor_systems = [];
    }
}

class SensorSystem {
    constructor() {
        self.sensor_system_id = null;
        self.sensor_node_id = null;
        self.sensor_system_metadata_effective_tsa = null;
        self.sensor_system_cost_band = null;
        self.sensor_system_description = null;
        self.sensor_system_deployed_by = null;
        self.sensor_system_deployment_date = null;
        self.sensor_system_deployment_notes = null;
        self.sensor_system_firmware_version = null;
        self.sensor_system_height_from_ground_meter = null;
        self.sensor_system_inlet_orientation = null;
        self.sensor_system_manufacturer_batch_number = null;
        self.sensor_system_manufacturer_name = null;
        self.sensor_system_model_name = null;
        self.sensor_system_model_version_name = null;
        self.sensor_system_origin_date = null;
        self.sensor_system_purchase_date = null;
        self.sensor_system_serial_number = null;
        self.sensor_system_source_id = null;
        self.sensor_system_attribution = null;
    }
}

class Sensor {
    constructor() {
        self.sensor_id = null;
        self.sensor_system_id = null;
        self.sensor_data_averaging_period = null;
        self.sensor_data_averaging_period_unit = null;
        self.sensor_data_logging_interval_second = null;
        self.sensor_lifecycle_stage = null;
        self.sensor_description = null;
        self.sensor_deployed_by = null;
        self.sensor_deployment_date = null;
        self.sensor_deactivation_date = null;
        self.sensor_deployment_notes = null;
        self.sensor_firmware_version = null;
        self.sensor_flow_rate_unit = null;
        self.sensor_flow_rate = null;
        self.sensor_calibration_procedure = null;
        self.sensor_last_calibration_timestamp = null;
        self.sensor_last_service_timestamp = null;
        self.sensor_manufacturer_batch_number = null;
        self.sensor_manufacturer_name = null;
        self.sensor_model_name = null;
        self.sensor_model_version_name = null;
        self.sensor_origin_date = null;
        self.sensor_purchase_date = null;
        self.sensor_sampling_duration = null;
        self.sensor_serial_number = null;
        self.sensor_measurand_id = null;
        self.sensor_source_id = null;
    }
}

module.exports = {
    Sensor,
    SensorNode,
    SensorSystem
}
