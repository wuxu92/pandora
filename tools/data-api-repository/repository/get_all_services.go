// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package repository

import (
	"fmt"
	"sync"
	"time"

	sdkModels "github.com/hashicorp/pandora/tools/data-api-sdk/v1/models"
)

// GetAllServices returns all the Services supported for this SourceDataType as a map of
// Service Name (key) to Service (value).
func (r *repositoryImpl) GetAllServices() (*map[string]sdkModels.Service, error) {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	output := make(map[string]sdkModels.Service)

	needBuildCache := make([]string, 0, len(r.availableDataSources))
	for serviceName := range r.availableDataSources {
		service, exists := r.cachedServices[serviceName]
		if !exists {
			needBuildCache = append(needBuildCache, serviceName)
		} else {
			output[serviceName] = service
		}
	}

	start := time.Now()
	defer func() {
		if len(needBuildCache) > 0 {
			r.logger.Info("GetAllServices took", "duration", time.Since(start), "buildCount", len(needBuildCache), "servicesCount", len(output))
		}
	}()

	var wg sync.WaitGroup
	var locker sync.Mutex
	var errors []error
	var emptyServices []string
	for _, serviceName := range needBuildCache {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			svc, err := r.loadService(name)
			if err != nil {
				r.logger.Error("Error loading service", "serviceName", name, "error", err)
				errors = append(errors, fmt.Errorf("error loading service %q: %w", name, err))
				return
			}
			if svc == nil {
				r.logger.Warn("Service not found", "serviceName", name)
				emptyServices = append(emptyServices, name)
				return
			}
			locker.Lock()
			defer locker.Unlock()
			r.cachedServices[name] = *svc
			output[name] = *svc
		}(serviceName)
	}
	wg.Wait()

	if len(errors) > 0 {
		return nil, fmt.Errorf("errors occurred while loading services: %v", errors)
	}

	if len(emptyServices) > 0 {
		r.logger.Warn("Some services were not found", "serviceNames", emptyServices)
		return nil, nil
	}

	return &output, nil

	// limit at 20 services at a time to avoid overwhelming the system
	// serviceCh := make(chan string, 20)
	// go func() {
	// 	for _, serviceName := range needBuildCache {
	// 		serviceCh <- serviceName
	// 	}
	// 	close(serviceCh)
	// }()

	// another goroutine to fetch from serviceCh and build the cache for the services with separate goroutine
	// var wg sync.WaitGroup
	// errCh := make(chan error)

	// wg.Add(len(needBuildCache))
	// go func() {
	// 	var locker sync.Mutex
	// 	for serviceName := range serviceCh {
	// 		go func(name string) {
	// 			defer wg.Done()
	// 			svc, err := r.loadService(name)
	// 			if err != nil {
	// 				r.logger.Error("Error loading service", "serviceName", name, "error", err)
	// 				errCh <- err
	// 				return
	// 			}
	// 			if svc == nil {
	// 				r.logger.Warn("Service not found", "serviceName", name)
	// 				return
	// 			}
	// 			locker.Lock()
	// 			defer locker.Unlock()
	// 			r.cachedServices[name] = *svc
	// 			output[name] = *svc
	// 		}(serviceName)
	// 	}
	// }()

	// doneCh := make(chan struct{})
	// go func() {
	// 	wg.Wait()
	// 	close(doneCh)
	// }()

	// // wait for all goroutines to finish: if wg.Done() or errCh has an error
	// select {
	// case <-errCh:
	// 	close(errCh)
	// 	return nil, <-errCh
	// case <-doneCh:
	// }

	// return &output, nil
}
