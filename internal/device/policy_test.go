package device

import "testing"

func TestTwinPolicyTypesAndConcretePaths(t *testing.T) {
	p := TwinPolicy{SiteID: "site", CommandTimeoutSeconds: 30, Paths: map[string]ManagedPath{"Device.Test.Value": {Type: "boolean", Writable: true}}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if !p.Allows("Device.Test.Value", ParameterValue{Type: "boolean", Value: "true"}, true) || p.Allows("Device.Test.Value", ParameterValue{Type: "boolean", Value: "not-bool"}, true) {
		t.Fatal("boolean validation")
	}
	for _, path := range []string{"Device.Test.*", "Device.Test.[Alias==x].Value", "Device.Reboot()", "Device.Test."} {
		p.Paths = map[string]ManagedPath{path: {Type: "string"}}
		if p.Validate() == nil {
			t.Fatal(path)
		}
	}
	for _, typ := range []string{"int", "unsignedInt"} {
		p.Paths = map[string]ManagedPath{"Device.Test.Value": {Type: typ, Writable: true}}
		if p.Allows("Device.Test.Value", ParameterValue{Type: typ, Value: "9999999999999999"}, true) {
			t.Fatal("overflow", typ)
		}
	}
}
