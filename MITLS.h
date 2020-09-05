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
			return;
		}
		repository->operator[](instance) = value;
	}

	T load(uintptr_t instance) const {
		if (!repository) { //check if the application has been already terminated, and we have an out of order destruction
			return T();
		}
		return repository->operator[](instance);
	}

	void remove(uintptr_t instance) {
		if (!repository) { //check if the application has been already terminated, and we have an out of order destruction
			return;
		}
		if (repository->find(instance) != repository->cend()) {
			repository->erase(instance);
		}
	}

	mi_tls_repository() {
		if (!repository) {
			repository = new std::unordered_map<uintptr_t, T>();
		}
	}

	~mi_tls_repository() {
		delete (repository);
		repository = nullptr;
	}

      private:
	//Key = memory location of the INSTANCE
	//Value = what you want to store
	//This is manual because thread_local are auto freed, but free order can be wrong! so when the DB closes (and remove the conn) we are in the wrong place
	inline static thread_local std::unordered_map<uintptr_t, T>* repository = nullptr;
};
