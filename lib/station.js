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

module.exports = {
    SensorNode,
    SensorSystem
}
