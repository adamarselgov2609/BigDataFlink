package ru.bigdata.flink.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MockDataDto implements Serializable {

    private Integer id;
    private String customer_first_name;
    private String customer_last_name;
    private Integer customer_age;
    private String customer_email;
    private String customer_country;
    private String customer_postal_code;
    private String customer_pet_type; 
    private String customer_pet_name; 
    private String customer_pet_breed; 


    private Integer sale_seller_id;
    private String seller_first_name;
    private String seller_last_name;
    private String seller_email;
    private String seller_country;
    private String seller_postal_code;


    private String store_name;
    private String store_location;
    private String store_country;
    private String store_state;
    private String store_city;
    private String store_phone;
    private String store_email;


    private String product_category;
    private String pet_category;
    private String product_name;
    private String product_color;
    private String product_size;
    private String product_brand;
    private String product_material;
    private String product_description;
    private Double product_price;
    private Double product_weight;
    private Double product_rating;
    private Integer product_reviews;
    private String product_release_date;
    private String product_expiry_date;


    private Integer product_quantity;  

  
    private String supplier_name;
    private String supplier_contact;
    private String supplier_email;
    private String supplier_phone;
    private String supplier_address;
    private String supplier_city;
    private String supplier_country;

    
    private Integer sale_customer_id;
    private Integer sale_product_id;
    private Integer sale_quantity;
    private Double sale_total_price;
    private String sale_date;
}
