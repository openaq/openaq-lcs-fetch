'use strict';

class SensorNode {
    constructor(p) {
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

        Object.assign(self, p);
    }

    json() {
        return {
            sensor_node_id: sensor_node_id,
            sensor_node_site_name: sensor_node_site_name,
            sensor_node_site_description: sensor_node_site_description,
            sensor_node_deployed_by: sensor_node_deployed_by,
            sensor_node_deployed_date: sensor_node_deployed_date,
            sensor_node_deploy_notes: sensor_node_deploy_notes,
            sensor_node_ismobile: sensor_node_ismobile,
            sensor_node_geometry: sensor_node_geometry,
            sensor_node_timezone: sensor_node_timezone,
            sensor_node_reporting_frequency: sensor_node_reporting_frequency,
            sensor_node_city: self.sensor_node_city,
            sensor_node_country: self.sensor_node_country
            sensor_systems = self.sensor_systems.map(s => s.json)
        }
    }
}

class SensorSystem {
    constructor(p) {
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

        Object.assign(self, p);
    }

    json() {
        return {
            sensor_system_id: self.sensor_system_id,
            sensor_node_id: self.sensor_node_id,
            sensor_system_metadata_effective_tsa: self.sensor_system_metadata_effective_tsa,
            sensor_system_cost_band: self.sensor_system_cost_band,
            sensor_system_description: self.sensor_system_description,
            sensor_system_deployed_by: self.sensor_system_deployed_by,
            sensor_system_deployment_date: self.sensor_system_deployment_date,
            sensor_system_deployment_notes: self.sensor_system_deployment_notes,
            sensor_system_firmware_version: self.sensor_system_firmware_version,
            sensor_system_height_from_ground_meter: self.sensor_system_height_from_ground_meter,
            sensor_system_inlet_orientation: self.sensor_system_inlet_orientation,
            sensor_system_manufacturer_batch_number: self.sensor_system_manufacturer_batch_number,
            sensor_system_manufacturer_name: self.sensor_system_manufacturer_name,
            sensor_system_model_name: self.sensor_system_model_name,
            sensor_system_model_version_name: self.sensor_system_model_version_name,
            sensor_system_origin_date: self.sensor_system_origin_date,
            sensor_system_purchase_date: self.sensor_system_purchase_date,
            sensor_system_serial_number: self.sensor_system_serial_number,
            sensor_system_source_id: self.sensor_system_source_id,
            sensor_system_attribution: self.sensor_system_attribution
        }
    }
}

class Sensor {
    constructor(p) {
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

        Object.assign(self, p);
    }
}

module.exports = {
    Sensor,
    SensorNode,
    SensorSystem
}
