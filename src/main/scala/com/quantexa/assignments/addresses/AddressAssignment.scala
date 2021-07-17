package com.quantexa.assignments.addresses

import scala.annotation.tailrec
import scala.io.Source

/***
  *  Given a dataset containing a list of addresses, along with customers who lived at the addresses
  *  and the start and end date that they lived there. Here is an example:
  *
  *     Customer ID 	Address ID 	From date 	To_date
  *     IND0003	      ADR001	    727	        803
  *     IND0004	      ADR003	    651	        820
  *     IND0007	      ADR003	    1710	      1825
  *     IND0008	      ADR005	    29	        191
  *
  *  Problem Statement
  *
  *  "For each address_id, find all of the customers who have lived at that address in overlapping times."
  *
  *
  */
  
  
object AddressAssignment extends App {


  //Defining a case classes
  case class AddressData(
                          customerId: String,
                          addressId: String,
                          fromDate: Int,
                          toDate: Int
                        )
  case class AddressGroupedData(
                                 group: Long,
                                 addressId: String,
                                 customerIds:Seq[String],
                                 fromDate: Int,
                                 toDate: Int
                                )

  val inputSource = Source.fromFile(getClass.getResource("/address_data.csv").getPath)
  val addressData: List[AddressData] = inputSource.getLines().drop(1).map {
    line =>
      val split = line.split(',')
      AddressData(split(0), split(1), split(2).toInt, split(3).toInt)
  }.toList

  inputSource.close()

  val sortedAddress=addressData.sortBy(x=>(x.addressId,x.fromDate))

  val emptyGroup = AddressGroupedData(0,"",List[String](), 0,0)

  val addressGroups = groupAddressData(sortedAddress, emptyGroup, List[AddressGroupedData]())

  addressGroups.sortBy(stay => (stay.addressId, stay.fromDate)).foreach(println)

  // Function to find overlapping groups
  @tailrec
  def groupAddressData(list: List[AddressData], current: AddressGroupedData, groupedData: List[AddressGroupedData]): List[AddressGroupedData] = {

    list match {
      case Nil =>
        current :: groupedData
      case address :: group =>
        // If new group or address
        if(current.addressId != address.addressId || address.fromDate > current.toDate) {
          val newGroup = if(current.addressId != "") current :: groupedData else groupedData
          // Start new group
          val newLatest = AddressGroupedData(
            current.group + 1,
            address.addressId,
            List(address.customerId),
            address.fromDate,
            address.toDate
          )
          groupAddressData(group, newLatest, newGroup)
        }
        // add data to existing list
        else {
          val newCurrentGroup = current.copy(toDate = math.max(address.toDate, current.toDate), customerIds = current.customerIds :+ address.customerId )
          groupAddressData(group, newCurrentGroup, groupedData)
        }

    }
  }

}
