#pragma once

#include <stdint.h>
#include <unordered_map>

// Forward declaration
template <typename T>
class mi_tls_repository;

template <typename T>
class mi_tls : protected mi_tls_repository<T> {
      public:
	mi_tls<T>() {
	}

	mi_tls<T>(const T& value) {
		this->store(reinterpret_cast<uintptr_t>(this), value);
	}

	mi_tls<T>& operator=(const T& value) {
		this->store(reinterpret_cast<uintptr_t>(this), value);
		return *this;
	}

	T get() {
		return this->load(reinterpret_cast<uintptr_t>(this));
	}

	operator T() {
		return this->load(reinterpret_cast<uintptr_t>(this));
	}

	~mi_tls() {
		this->remove(reinterpret_cast<uintptr_t>(this));
	}
};

template <typename T>
class mi_tls_repository {
      protected:
	void store(uintptr_t instance, T value) {
		if (!repository) { //check if the application has been already terminated, and we have an out of order destruction
			repository = new std::unordered_map<uintptr_t, T>();
		}
		repository->operator[](instance) = value;
	}

	T load(uintptr_t instance) const {
		if (!repository) { //check if the application has been already terminated, and we have an out of order destruction
			repository = new std::unordered_map<uintptr_t, T>();
		}
		return repository->operator[](instance);
	}

	void remove(uintptr_t instance) {
		if (!repository) { //check if the application has been already terminated, and we have an out of order destruction
			repository = new std::unordered_map<uintptr_t, T>();
		}
		if (repository->find(instance) != repository->cend()) {
			repository->erase(instance);
		}
	}

	//This will be called ONLY for the first thread
	mi_tls_repository() {
		if (!repository) {
			repository = new std::unordered_map<uintptr_t, T>();
		}
	}

	//again this has no access to all the various thread local storage
	//TODO add
	~mi_tls_repository() {
		if (repository) {
			delete (repository);
			repository = nullptr;
		}
	}

      private:
	//Key = memory location of the INSTANCE
	//Value = what you want to store
	//This is manual because thread_local are auto freed, but free order can be wrong! so when the DB closes (and remove the conn) we are in the wrong place
	//Therefore this will leak memory, once you close the program, so is 100% irrelevant
	inline static thread_local std::unordered_map<uintptr_t, T>* repository = nullptr;
};
